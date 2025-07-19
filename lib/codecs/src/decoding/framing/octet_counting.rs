use std::io;

use bytes::{Buf, Bytes, BytesMut};
use derivative::Derivative;
use tokio_util::codec::{LinesCodec, LinesCodecError};
use tracing::trace;
use vector_config::configurable_component;

use super::BoxedFramingError;

/// Config used to build a `OctetCountingDecoder`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct OctetCountingDecoderConfig {
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    /// Options for the octet counting decoder.
    pub octet_counting: OctetCountingDecoderOptions,
}

impl OctetCountingDecoderConfig {
    /// Build the `OctetCountingDecoder` from this configuration.
    pub fn build(&self) -> OctetCountingDecoder {
        if let Some(max_length) = self.octet_counting.max_length {
            OctetCountingDecoder::new_with_max_length(max_length)
        } else {
            OctetCountingDecoder::new()
        }
    }
}

/// Options for building a `OctetCountingDecoder`.
#[configurable_component]
#[derive(Clone, Debug, Derivative, PartialEq, Eq)]
#[derivative(Default)]
pub struct OctetCountingDecoderOptions {
    /// The maximum length of the byte buffer.
    #[serde(skip_serializing_if = "vector_core::serde::is_default")]
    pub max_length: Option<usize>,
}

/// Codec using the `Octet Counting` format as specified in
/// <https://tools.ietf.org/html/rfc6587#section-3.4.1>.
#[derive(Clone, Debug)]
pub struct OctetCountingDecoder {
    other: LinesCodec,
    octet_decoding: Option<State>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    NotDiscarding,
    Discarding(usize),
    DiscardingToEol,
}

impl OctetCountingDecoder {
    /// Creates a new `OctetCountingDecoder`.
    pub fn new() -> Self {
        Self {
            other: LinesCodec::new(),
            octet_decoding: None,
        }
    }

    /// Creates a `OctetCountingDecoder` with a maximum frame length limit.
    pub fn new_with_max_length(max_length: usize) -> Self {
        Self {
            other: LinesCodec::new_with_max_length(max_length),
            octet_decoding: None,
        }
    }

    /// Decode a frame.
    fn octet_decode(
        &mut self,
        state: State,
        src: &mut BytesMut,
    ) -> Result<Option<Bytes>, LinesCodecError> {
        // Encoding scheme:
        //
        // len ' ' data
        // |    |  | len number of bytes that contain syslog message
        // |    |
        // |    | Separating whitespace
        // |
        // | ASCII decimal number of unknown length

        let space_pos = src.iter().position(|&b| b == b' ');

        // If we are discarding, discard to the next newline.
        let newline_pos = src.iter().position(|&b| b == b'\n');

        match (state, newline_pos, space_pos) {
            (State::Discarding(chars), _, _) if src.len() >= chars => {
                // We have a certain number of chars to discard.
                //
                // There are enough chars in this frame to discard
                src.advance(chars);
                self.octet_decoding = None;
                Err(LinesCodecError::Io(io::Error::other(
                    "Frame length limit exceeded",
                )))
            }

            (State::Discarding(chars), _, _) => {
                // We have a certain number of chars to discard.
                //
                // There aren't enough in this frame so we need to discard the
                // entire frame and adjust the amount to discard accordingly.
                self.octet_decoding = Some(State::Discarding(src.len() - chars));
                src.advance(src.len());
                Ok(None)
            }

            (State::DiscardingToEol, Some(offset), _) => {
                // When discarding we keep discarding to the next newline.
                src.advance(offset + 1);
                self.octet_decoding = None;
                Err(LinesCodecError::Io(io::Error::other(
                    "Frame length limit exceeded",
                )))
            }

            (State::DiscardingToEol, None, _) => {
                // There is no newline in this frame.
                //
                // Since we don't have a set number of chars we want to discard,
                // we need to discard to the next newline. Advance as far as we
                // can to discard the entire frame.
                src.advance(src.len());
                Ok(None)
            }

            (State::NotDiscarding, _, Some(space_pos)) if space_pos < self.other.max_length() => {
                // Everything looks good.
                //
                // We aren't discarding, we have a space that is not beyond our
                // maximum length. Attempt to parse the bytes as a number which
                // will hopefully give us a sensible length for our message.
                let len: usize = match std::str::from_utf8(&src[..space_pos])
                    .map_err(|_| ())
                    .and_then(|num| num.parse().map_err(|_| ()))
                {
                    Ok(len) => len,
                    Err(_) => {
                        // It was not a sensible number.
                        //
                        // Advance the buffer past the erroneous bytes to
                        // prevent us getting stuck in an infinite loop.
                        src.advance(space_pos + 1);
                        self.octet_decoding = None;
                        return Err(LinesCodecError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Unable to decode message len as number",
                        )));
                    }
                };

                let from = space_pos + 1;
                let to = from + len;

                if len > self.other.max_length() {
                    // The length is greater than we want.
                    //
                    // We need to discard the entire message.
                    self.octet_decoding = Some(State::Discarding(len));
                    src.advance(space_pos + 1);

                    Ok(None)
                } else if let Some(msg) = src.get(from..to) {
                    let bytes = match std::str::from_utf8(msg) {
                        Ok(_) => Bytes::copy_from_slice(msg),
                        Err(_) => {
                            // The data was not valid UTF8 :-(.
                            //
                            // Advance the buffer past the erroneous bytes to
                            // prevent us getting stuck in an infinite loop.
                            src.advance(to);
                            self.octet_decoding = None;
                            return Err(LinesCodecError::Io(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Unable to decode message as UTF8",
                            )));
                        }
                    };

                    // We have managed to read the entire message as valid UTF8!
                    src.advance(to);
                    self.octet_decoding = None;
                    Ok(Some(bytes))
                } else {
                    // We have an acceptable number of bytes in this message,
                    // but not all the data was in the frame.
                    //
                    // Return `None` to indicate we want more data before we do
                    // anything else.
                    Ok(None)
                }
            }

            (State::NotDiscarding, Some(newline_pos), _) => {
                // Beyond maximum length, advance to the newline.
                src.advance(newline_pos + 1);
                Err(LinesCodecError::Io(io::Error::other(
                    "Frame length limit exceeded",
                )))
            }

            (State::NotDiscarding, None, _) if src.len() < self.other.max_length() => {
                // We aren't discarding, but there is no useful character to
                // tell us what to do next.
                //
                // We are still not beyond the max length, so just return `None`
                // to indicate we need to wait for more data.
                Ok(None)
            }

            (State::NotDiscarding, None, _) => {
                // There is no newline in this frame and we have more data than
                // we want to handle.
                //
                // Advance as far as we can to discard the entire frame.
                self.octet_decoding = Some(State::DiscardingToEol);
                src.advance(src.len());
                Ok(None)
            }
        }
    }

    /// `None` if this is not octet counting encoded.
    fn checked_decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Option<Result<Option<Bytes>, LinesCodecError>> {
        if let Some(&first_byte) = src.first() {
            if (49..=57).contains(&first_byte) {
                // First character is non zero number so we can assume that
                // octet count framing is used.
                trace!("Octet counting encoded event detected.");
                self.octet_decoding = Some(State::NotDiscarding);
            }
        }

        self.octet_decoding
            .map(|state| self.octet_decode(state, src))
    }
}

impl Default for OctetCountingDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl tokio_util::codec::Decoder for OctetCountingDecoder {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(ret) = self.checked_decode(src) {
            ret
        } else {
            // Octet counting isn't used so fallback to newline codec.
            self.other
                .decode(src)
                .map(|line| line.map(|line| line.into()))
        }
        .map_err(Into::into)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(ret) = self.checked_decode(buf) {
            ret
        } else {
            // Octet counting isn't used so fallback to newline codec.
            self.other
                .decode_eof(buf)
                .map(|line| line.map(|line| line.into()))
        }
        .map_err(Into::into)
    }
}

