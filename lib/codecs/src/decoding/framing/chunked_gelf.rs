use super::{BoxedFramingError, FramingError};
use crate::{BytesDecoder, StreamDecodingError};
use bytes::{Buf, Bytes, BytesMut};
use derivative::Derivative;
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use snafu::{ensure, ResultExt, Snafu};
use std::any::Any;
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio;
use tokio::task::JoinHandle;
use tokio_util::codec::Decoder;
use tracing::{debug, trace, warn};
use vector_common::constants::{GZIP_MAGIC, ZLIB_MAGIC};
use vector_config::configurable_component;

const GELF_MAGIC: &[u8] = &[0x1e, 0x0f];
const GELF_MAX_TOTAL_CHUNKS: u8 = 128;
const DEFAULT_TIMEOUT_SECS: f64 = 5.0;

const fn default_timeout_secs() -> f64 {
    DEFAULT_TIMEOUT_SECS
}

/// Config used to build a `ChunkedGelfDecoder`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct ChunkedGelfDecoderConfig {
    /// Options for the chunked GELF decoder.
    #[serde(default)]
    pub chunked_gelf: ChunkedGelfDecoderOptions,
}

impl ChunkedGelfDecoderConfig {
    /// Build the `ChunkedGelfDecoder` from this configuration.
    pub fn build(&self) -> ChunkedGelfDecoder {
        ChunkedGelfDecoder::new(
            self.chunked_gelf.timeout_secs,
            self.chunked_gelf.pending_messages_limit,
            self.chunked_gelf.max_length,
            self.chunked_gelf.decompression,
        )
    }
}

/// Options for building a `ChunkedGelfDecoder`.
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
pub struct ChunkedGelfDecoderOptions {
    /// The timeout, in seconds, for a message to be fully received. If the timeout is reached, the
    /// decoder drops all the received chunks of the timed out message.
    #[serde(default = "default_timeout_secs")]
    #[derivative(Default(value = "default_timeout_secs()"))]
    pub timeout_secs: f64,

    /// The maximum number of pending incomplete messages. If this limit is reached, the decoder starts
    /// dropping chunks of new messages, ensuring the memory usage of the decoder's state is bounded.
    /// If this option is not set, the decoder does not limit the number of pending messages and the memory usage
    /// of its messages buffer can grow unbounded. This matches Graylog Server's behavior.
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    pub pending_messages_limit: Option<usize>,

    /// The maximum length of a single GELF message, in bytes. Messages longer than this length will
    /// be dropped. If this option is not set, the decoder does not limit the length of messages and
    /// the per-message memory is unbounded.
    ///
    /// Note that a message can be composed of multiple chunks and this limit is applied to the whole
    /// message, not to individual chunks.
    ///
    /// This limit takes only into account the message's payload and the GELF header bytes are excluded from the calculation.
    /// The message's payload is the concatenation of all the chunks' payloads.
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    pub max_length: Option<usize>,

    /// Decompression configuration for GELF messages.
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    pub decompression: ChunkedGelfDecompressionConfig,
}

/// Decompression options for ChunkedGelfDecoder.
#[configurable_component]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Derivative)]
#[derivative(Default)]
pub enum ChunkedGelfDecompressionConfig {
    /// Automatically detect the decompression method based on the magic bytes of the message.
    #[derivative(Default)]
    Auto,
    /// Use Gzip decompression.
    Gzip,
    /// Use Zlib decompression.
    Zlib,
    /// Do not decompress the message.
    None,
}

impl ChunkedGelfDecompressionConfig {
    pub fn get_decompression(&self, data: &Bytes) -> ChunkedGelfDecompression {
        match self {
            Self::Auto => ChunkedGelfDecompression::from_magic(data),
            Self::Gzip => ChunkedGelfDecompression::Gzip,
            Self::Zlib => ChunkedGelfDecompression::Zlib,
            Self::None => ChunkedGelfDecompression::None,
        }
    }
}

#[derive(Debug)]
struct MessageState {
    total_chunks: u8,
    chunks: [Bytes; GELF_MAX_TOTAL_CHUNKS as usize],
    chunks_bitmap: u128,
    current_length: usize,
    timeout_task: JoinHandle<()>,
}

impl MessageState {
    pub const fn new(total_chunks: u8, timeout_task: JoinHandle<()>) -> Self {
        Self {
            total_chunks,
            chunks: [const { Bytes::new() }; GELF_MAX_TOTAL_CHUNKS as usize],
            chunks_bitmap: 0,
            current_length: 0,
            timeout_task,
        }
    }

    fn is_chunk_present(&self, sequence_number: u8) -> bool {
        let chunk_bitmap_id = 1 << sequence_number;
        self.chunks_bitmap & chunk_bitmap_id != 0
    }

    fn add_chunk(&mut self, sequence_number: u8, chunk: Bytes) {
        let chunk_bitmap_id = 1 << sequence_number;
        self.chunks_bitmap |= chunk_bitmap_id;
        self.current_length += chunk.remaining();
        self.chunks[sequence_number as usize] = chunk;
    }

    fn is_complete(&self) -> bool {
        self.chunks_bitmap.count_ones() == self.total_chunks as u32
    }

    fn current_length(&self) -> usize {
        self.current_length
    }

    fn retrieve_message(&self) -> Option<Bytes> {
        if self.is_complete() {
            self.timeout_task.abort();
            let chunks = &self.chunks[0..self.total_chunks as usize];
            let mut message = BytesMut::new();
            for chunk in chunks {
                message.extend_from_slice(chunk);
            }
            Some(message.freeze())
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChunkedGelfDecompression {
    Gzip,
    Zlib,
    None,
}

impl ChunkedGelfDecompression {
    pub fn from_magic(data: &Bytes) -> Self {
        if data.starts_with(GZIP_MAGIC) {
            trace!("Detected Gzip compression");
            return Self::Gzip;
        }

        if data.starts_with(ZLIB_MAGIC) {
            // Based on https://datatracker.ietf.org/doc/html/rfc1950#section-2.2
            if let Some([first_byte, second_byte]) = data.get(0..2) {
                if (*first_byte as u16 * 256 + *second_byte as u16) % 31 == 0 {
                    trace!("Detected Zlib compression");
                    return Self::Zlib;
                }
            };

            warn!(
                "Detected Zlib magic bytes but the header is invalid: {:?}",
                data.get(0..2)
            );
        };

        trace!("No compression detected",);
        Self::None
    }

    pub fn decompress(&self, data: Bytes) -> Result<Bytes, ChunkedGelfDecompressionError> {
        let decompressed = match self {
            Self::Gzip => {
                let mut decoder = MultiGzDecoder::new(data.reader());
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .context(GzipDecompressionSnafu)?;
                Bytes::from(decompressed)
            }
            Self::Zlib => {
                let mut decoder = ZlibDecoder::new(data.reader());
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .context(ZlibDecompressionSnafu)?;
                Bytes::from(decompressed)
            }
            Self::None => data,
        };
        Ok(decompressed)
    }
}

#[derive(Debug, Snafu)]
pub enum ChunkedGelfDecompressionError {
    #[snafu(display("Gzip decompression error: {source}"))]
    GzipDecompression { source: std::io::Error },
    #[snafu(display("Zlib decompression error: {source}"))]
    ZlibDecompression { source: std::io::Error },
}

#[derive(Debug, Snafu)]
pub enum ChunkedGelfDecoderError {
    #[snafu(display("Invalid chunk header with less than 10 bytes: 0x{header:0x}"))]
    InvalidChunkHeader { header: Bytes },
    #[snafu(display("Received chunk with message id {message_id} and sequence number {sequence_number} has an invalid total chunks value of {total_chunks}. It must be between 1 and {GELF_MAX_TOTAL_CHUNKS}."))]
    InvalidTotalChunks {
        message_id: u64,
        sequence_number: u8,
        total_chunks: u8,
    },
    #[snafu(display("Received chunk with message id {message_id} and sequence number {sequence_number} has a sequence number greater than its total chunks value of {total_chunks}"))]
    InvalidSequenceNumber {
        message_id: u64,
        sequence_number: u8,
        total_chunks: u8,
    },
    #[snafu(display("Pending messages limit of {pending_messages_limit} reached while processing chunk with message id {message_id} and sequence number {sequence_number}"))]
    PendingMessagesLimitReached {
        message_id: u64,
        sequence_number: u8,
        pending_messages_limit: usize,
    },
    #[snafu(display("Received chunk with message id {message_id} and sequence number {sequence_number} has different total chunks values: original total chunks value is {original_total_chunks} and received total chunks value is {received_total_chunks}"))]
    TotalChunksMismatch {
        message_id: u64,
        sequence_number: u8,
        original_total_chunks: u8,
        received_total_chunks: u8,
    },
    #[snafu(display("Message with id {message_id} has exceeded the maximum message length and it will be dropped: got {length} bytes and max message length is {max_length} bytes. Discarding all buffered chunks of that message"))]
    MaxLengthExceed {
        message_id: u64,
        sequence_number: u8,
        length: usize,
        max_length: usize,
    },
    #[snafu(display("Error while decompressing message. {source}"))]
    Decompression {
        source: ChunkedGelfDecompressionError,
    },
}

impl StreamDecodingError for ChunkedGelfDecoderError {
    fn can_continue(&self) -> bool {
        true
    }
}

impl FramingError for ChunkedGelfDecoderError {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

/// A codec for handling GELF messages that may be chunked. The implementation is based on [Graylog's GELF documentation](https://go2docs.graylog.org/5-0/getting_in_log_data/gelf.html#GELFviaUDP)
/// and [Graylog's go-gelf library](https://github.com/Graylog2/go-gelf/blob/v1/gelf/reader.go).
#[derive(Debug, Clone)]
pub struct ChunkedGelfDecoder {
    // We have to use this decoder to read all the bytes from the buffer first and don't let tokio
    // read it buffered, as tokio FramedRead will not always call the decode method with the
    // whole message. (see https://docs.rs/tokio-util/latest/src/tokio_util/codec/framed_impl.rs.html#26).
    // This limitation is due to the fact that the GELF format does not specify the length of the
    // message, so we have to read all the bytes from the message (datagram)
    bytes_decoder: BytesDecoder,
    decompression_config: ChunkedGelfDecompressionConfig,
    state: Arc<Mutex<HashMap<u64, MessageState>>>,
    timeout: Duration,
    pending_messages_limit: Option<usize>,
    max_length: Option<usize>,
}

impl ChunkedGelfDecoder {
    /// Creates a new `ChunkedGelfDecoder`.
    pub fn new(
        timeout_secs: f64,
        pending_messages_limit: Option<usize>,
        max_length: Option<usize>,
        decompression_config: ChunkedGelfDecompressionConfig,
    ) -> Self {
        Self {
            bytes_decoder: BytesDecoder::new(),
            decompression_config,
            state: Arc::new(Mutex::new(HashMap::new())),
            timeout: Duration::from_secs_f64(timeout_secs),
            pending_messages_limit,
            max_length,
        }
    }

    /// Decode a GELF chunk
    pub fn decode_chunk(
        &mut self,
        mut chunk: Bytes,
    ) -> Result<Option<Bytes>, ChunkedGelfDecoderError> {
        // Encoding scheme:
        //
        // +------------+-----------------+--------------+----------------------+
        // | Message id | Sequence number | Total chunks |    Chunk payload     |
        // +------------+-----------------+--------------+----------------------+
        // | 64 bits    | 8 bits          | 8 bits       | remaining bits       |
        // +------------+-----------------+--------------+----------------------+
        //
        // As this codec is oriented for UDP, the chunks (datagrams) are not guaranteed to be received in order,
        // nor to be received at all. So, we have to store the chunks in a buffer (state field) until we receive
        // all the chunks of a message. When we receive all the chunks of a message, we can concatenate them
        // and return the complete payload.

        // We need 10 bytes to read the message id, sequence number and total chunks
        ensure!(
            chunk.remaining() >= 10,
            InvalidChunkHeaderSnafu { header: chunk }
        );

        let message_id = chunk.get_u64();
        let sequence_number = chunk.get_u8();
        let total_chunks = chunk.get_u8();

        ensure!(
            total_chunks > 0 && total_chunks <= GELF_MAX_TOTAL_CHUNKS,
            InvalidTotalChunksSnafu {
                message_id,
                sequence_number,
                total_chunks
            }
        );

        ensure!(
            sequence_number < total_chunks,
            InvalidSequenceNumberSnafu {
                message_id,
                sequence_number,
                total_chunks
            }
        );

        let mut state_lock = self.state.lock().expect("poisoned lock");

        if let Some(pending_messages_limit) = self.pending_messages_limit {
            ensure!(
                state_lock.len() < pending_messages_limit,
                PendingMessagesLimitReachedSnafu {
                    message_id,
                    sequence_number,
                    pending_messages_limit
                }
            );
        }

        let message_state = state_lock.entry(message_id).or_insert_with(|| {
            // We need to spawn a task that will clear the message state after a certain time
            // otherwise we will have a memory leak due to messages that never complete
            let state = Arc::clone(&self.state);
            let timeout = self.timeout;
            let timeout_handle = tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                let mut state_lock = state.lock().expect("poisoned lock");
                if state_lock.remove(&message_id).is_some() {
                    warn!(
                        message_id = message_id,
                        timeout_secs = timeout.as_secs_f64(),
                        "Message was not fully received within the timeout window. Discarding it."
                    );
                }
            });
            MessageState::new(total_chunks, timeout_handle)
        });

        ensure!(
            message_state.total_chunks == total_chunks,
            TotalChunksMismatchSnafu {
                message_id,
                sequence_number,
                original_total_chunks: message_state.total_chunks,
                received_total_chunks: total_chunks
            }
        );

        if message_state.is_chunk_present(sequence_number) {
            debug!(
                message_id = message_id,
                sequence_number = sequence_number,
                "Received a duplicate chunk. Ignoring it."
            );
            return Ok(None);
        }

        message_state.add_chunk(sequence_number, chunk);

        if let Some(max_length) = self.max_length {
            let length = message_state.current_length();
            if length > max_length {
                state_lock.remove(&message_id);
                return Err(ChunkedGelfDecoderError::MaxLengthExceed {
                    message_id,
                    sequence_number,
                    length,
                    max_length,
                });
            }
        }

        if let Some(message) = message_state.retrieve_message() {
            state_lock.remove(&message_id);
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    /// Decode a GELF message that may be chunked or not. The source bytes are expected to be
    /// datagram-based (or message-based), so it must not contain multiple GELF messages
    /// delimited by '\0', such as it would be in a stream-based protocol.
    pub fn decode_message(
        &mut self,
        mut src: Bytes,
    ) -> Result<Option<Bytes>, ChunkedGelfDecoderError> {
        let message = if src.starts_with(GELF_MAGIC) {
            trace!("Received a chunked GELF message based on the magic bytes");
            src.advance(2);
            self.decode_chunk(src)?
        } else {
            trace!(
                "Received an unchunked GELF message. First two bytes of message: {:?}",
                &src[0..2]
            );
            Some(src)
        };

        // We can have both chunked and unchunked messages that are compressed
        message
            .map(|message| {
                self.decompression_config
                    .get_decompression(&message)
                    .decompress(message)
                    .context(DecompressionSnafu)
            })
            .transpose()
    }
}

impl Default for ChunkedGelfDecoder {
    fn default() -> Self {
        Self::new(
            DEFAULT_TIMEOUT_SECS,
            None,
            None,
            ChunkedGelfDecompressionConfig::Auto,
        )
    }
}

impl Decoder for ChunkedGelfDecoder {
    type Item = Bytes;

    type Error = BoxedFramingError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        Ok(self
            .bytes_decoder
            .decode(src)?
            .and_then(|frame| self.decode_message(frame).transpose())
            .transpose()?)
    }
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        Ok(self
            .bytes_decoder
            .decode_eof(buf)?
            .and_then(|frame| self.decode_message(frame).transpose())
            .transpose()?)
    }
}

