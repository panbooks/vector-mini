use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::Decoder;

use super::BoxedFramingError;

/// Config used to build a `BytesDecoderConfig`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BytesDecoderConfig;

impl BytesDecoderConfig {
    /// Creates a new `BytesDecoderConfig`.
    pub const fn new() -> Self {
        Self
    }

    /// Build the `ByteDecoder` from this configuration.
    pub const fn build(&self) -> BytesDecoder {
        BytesDecoder::new()
    }
}

/// A decoder for passing through bytes as-is.
///
/// This is basically a no-op and is used to convert from `BytesMut` to `Bytes`.
#[derive(Debug, Clone)]
pub struct BytesDecoder {
    /// Whether the empty buffer has been flushed. This is important to
    /// propagate empty frames in message based transports.
    flushed: bool,
}

impl BytesDecoder {
    /// Creates a new `BytesDecoder`.
    pub const fn new() -> Self {
        Self { flushed: false }
    }
}

impl Default for BytesDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for BytesDecoder {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.flushed = false;
        Ok(None)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.flushed && src.is_empty() {
            Ok(None)
        } else {
            self.flushed = true;
            let frame = src.split();
            Ok(Some(frame.freeze()))
        }
    }
}

