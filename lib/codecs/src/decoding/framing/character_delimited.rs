use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr;
use tokio_util::codec::Decoder;
use tracing::{trace, warn};
use vector_config::configurable_component;

use super::BoxedFramingError;

/// Config used to build a `CharacterDelimitedDecoder`.
#[configurable_component]
#[derive(Debug, Clone)]
pub struct CharacterDelimitedDecoderConfig {
    /// Options for the character delimited decoder.
    pub character_delimited: CharacterDelimitedDecoderOptions,
}

impl CharacterDelimitedDecoderConfig {
    /// Creates a `CharacterDelimitedDecoderConfig` with the specified delimiter and default max length.
    pub const fn new(delimiter: u8) -> Self {
        Self {
            character_delimited: CharacterDelimitedDecoderOptions::new(delimiter, None),
        }
    }
    /// Build the `CharacterDelimitedDecoder` from this configuration.
    pub const fn build(&self) -> CharacterDelimitedDecoder {
        if let Some(max_length) = self.character_delimited.max_length {
            CharacterDelimitedDecoder::new_with_max_length(
                self.character_delimited.delimiter,
                max_length,
            )
        } else {
            CharacterDelimitedDecoder::new(self.character_delimited.delimiter)
        }
    }
}

/// Options for building a `CharacterDelimitedDecoder`.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CharacterDelimitedDecoderOptions {
    /// The character that delimits byte sequences.
    #[configurable(metadata(docs::type_override = "ascii_char"))]
    #[serde(with = "vector_core::serde::ascii_char")]
    pub delimiter: u8,

    /// The maximum length of the byte buffer.
    ///
    /// This length does *not* include the trailing delimiter.
    ///
    /// By default, there is no maximum length enforced. If events are malformed, this can lead to
    /// additional resource usage as events continue to be buffered in memory, and can potentially
    /// lead to memory exhaustion in extreme cases.
    ///
    /// If there is a risk of processing malformed data, such as logs with user-controlled input,
    /// consider setting the maximum length to a reasonably large value as a safety net. This
    /// ensures that processing is not actually unbounded.
    #[serde(skip_serializing_if = "vector_core::serde::is_default")]
    pub max_length: Option<usize>,
}

impl CharacterDelimitedDecoderOptions {
    /// Create a `CharacterDelimitedDecoderOptions` with a delimiter and optional max_length.
    pub const fn new(delimiter: u8, max_length: Option<usize>) -> Self {
        Self {
            delimiter,
            max_length,
        }
    }
}

/// A decoder for handling bytes that are delimited by (a) chosen character(s).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CharacterDelimitedDecoder {
    /// The delimiter used to separate byte sequences.
    pub delimiter: u8,
    /// The maximum length of the byte buffer.
    pub max_length: usize,
}

impl CharacterDelimitedDecoder {
    /// Creates a `CharacterDelimitedDecoder` with the specified delimiter.
    pub const fn new(delimiter: u8) -> Self {
        CharacterDelimitedDecoder {
            delimiter,
            max_length: usize::MAX,
        }
    }

    /// Creates a `CharacterDelimitedDecoder` with a maximum frame length limit.
    ///
    /// Any frames longer than `max_length` bytes will be discarded entirely.
    pub const fn new_with_max_length(delimiter: u8, max_length: usize) -> Self {
        CharacterDelimitedDecoder {
            max_length,
            ..CharacterDelimitedDecoder::new(delimiter)
        }
    }

    /// Returns the maximum frame length when decoding.
    pub const fn max_length(&self) -> usize {
        self.max_length
    }
}

impl Decoder for CharacterDelimitedDecoder {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, Self::Error> {
        loop {
            // This function has the following goal: we are searching for
            // sub-buffers delimited by `self.delimiter` with size no more than
            // `self.max_length`. If a sub-buffer is found that exceeds
            // `self.max_length` we discard it, else we return it. At the end of
            // the buffer if the delimiter is not present the remainder of the
            // buffer is discarded.
            match memchr(self.delimiter, buf) {
                None => return Ok(None),
                Some(next_delimiter_idx) => {
                    if next_delimiter_idx > self.max_length {
                        // The discovered sub-buffer is too big, so we discard
                        // it, taking care to also discard the delimiter.
                        warn!(
                            message = "Discarding frame larger than max_length.",
                            buf_len = buf.len(),
                            max_length = self.max_length,
                            internal_log_rate_limit = true
                        );
                        buf.advance(next_delimiter_idx + 1);
                    } else {
                        let frame = buf.split_to(next_delimiter_idx).freeze();
                        trace!(
                            message = "Decoding the frame.",
                            bytes_processed = frame.len()
                        );
                        buf.advance(1); // scoot past the delimiter
                        return Ok(Some(frame));
                    }
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, Self::Error> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else if buf.len() > self.max_length {
                    warn!(
                        message = "Discarding frame larger than max_length.",
                        buf_len = buf.len(),
                        max_length = self.max_length,
                        internal_log_rate_limit = true
                    );
                    Ok(None)
                } else {
                    let bytes: Bytes = buf.split_to(buf.len()).freeze();
                    Ok(Some(bytes))
                }
            }
        }
    }
}

