use bytes::{Bytes, BytesMut};
use derivative::Derivative;
use tokio_util::codec::Decoder;
use vector_config::configurable_component;

use crate::common::length_delimited::LengthDelimitedCoderOptions;

use super::BoxedFramingError;

/// Config used to build a `LengthDelimitedDecoder`.
#[configurable_component]
#[derive(Debug, Clone, Derivative)]
#[derivative(Default)]
pub struct LengthDelimitedDecoderConfig {
    /// Options for the length delimited decoder.
    #[serde(skip_serializing_if = "vector_core::serde::is_default")]
    pub length_delimited: LengthDelimitedCoderOptions,
}

impl LengthDelimitedDecoderConfig {
    /// Build the `LengthDelimitedDecoder` from this configuration.
    pub fn build(&self) -> LengthDelimitedDecoder {
        LengthDelimitedDecoder::new(&self.length_delimited)
    }
}

/// A codec for handling bytes sequences whose length is encoded in a frame head.
#[derive(Debug, Clone)]
pub struct LengthDelimitedDecoder(tokio_util::codec::LengthDelimitedCodec);

impl LengthDelimitedDecoder {
    /// Creates a new `LengthDelimitedDecoder`.
    pub fn new(config: &LengthDelimitedCoderOptions) -> Self {
        Self(config.build_codec())
    }
}

impl Default for LengthDelimitedDecoder {
    fn default() -> Self {
        Self(tokio_util::codec::LengthDelimitedCodec::new())
    }
}

impl Decoder for LengthDelimitedDecoder {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.0
            .decode(src)
            .map(|bytes| bytes.map(BytesMut::freeze))
            .map_err(Into::into)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.0
            .decode_eof(src)
            .map(|bytes| bytes.map(BytesMut::freeze))
            .map_err(Into::into)
    }
}

