use crate::codecs::Transformer;
use vector_lib::codecs::{
    encoding::{Framer, FramingConfig, Serializer, SerializerConfig},
    CharacterDelimitedEncoder, LengthDelimitedEncoder, NewlineDelimitedEncoder,
};
use vector_lib::configurable::configurable_component;

/// Encoding configuration.
#[configurable_component]
#[derive(Clone, Debug)]
/// Configures how events are encoded into raw bytes.
/// The selected encoding also determines which input types (logs, metrics, traces) are supported.
pub struct EncodingConfig {
    #[serde(flatten)]
    encoding: SerializerConfig,

    #[serde(flatten)]
    transformer: Transformer,
}

impl EncodingConfig {
    /// Creates a new `EncodingConfig` with the provided `SerializerConfig` and `Transformer`.
    pub const fn new(encoding: SerializerConfig, transformer: Transformer) -> Self {
        Self {
            encoding,
            transformer,
        }
    }

    /// Build a `Transformer` that applies the encoding rules to an event before serialization.
    pub fn transformer(&self) -> Transformer {
        self.transformer.clone()
    }

    /// Get the encoding configuration.
    pub const fn config(&self) -> &SerializerConfig {
        &self.encoding
    }

    /// Build the `Serializer` for this config.
    pub fn build(&self) -> crate::Result<Serializer> {
        self.encoding.build()
    }
}

impl<T> From<T> for EncodingConfig
where
    T: Into<SerializerConfig>,
{
    fn from(encoding: T) -> Self {
        Self {
            encoding: encoding.into(),
            transformer: Default::default(),
        }
    }
}

/// Encoding configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct EncodingConfigWithFraming {
    #[configurable(derived)]
    framing: Option<FramingConfig>,

    #[configurable(derived)]
    encoding: EncodingConfig,
}

impl EncodingConfigWithFraming {
    /// Creates a new `EncodingConfigWithFraming` with the provided `FramingConfig`,
    /// `SerializerConfig` and `Transformer`.
    pub const fn new(
        framing: Option<FramingConfig>,
        encoding: SerializerConfig,
        transformer: Transformer,
    ) -> Self {
        Self {
            framing,
            encoding: EncodingConfig {
                encoding,
                transformer,
            },
        }
    }

    /// Build a `Transformer` that applies the encoding rules to an event before serialization.
    pub fn transformer(&self) -> Transformer {
        self.encoding.transformer.clone()
    }

    /// Get the encoding configuration.
    pub const fn config(&self) -> (&Option<FramingConfig>, &SerializerConfig) {
        (&self.framing, &self.encoding.encoding)
    }

    /// Build the `Framer` and `Serializer` for this config.
    pub fn build(&self, sink_type: SinkType) -> crate::Result<(Framer, Serializer)> {
        let framer = self.framing.as_ref().map(|framing| framing.build());
        let serializer = self.encoding.build()?;

        let framer = match (framer, &serializer) {
            (Some(framer), _) => framer,
            (None, Serializer::Json(_)) => match sink_type {
                SinkType::StreamBased => NewlineDelimitedEncoder::default().into(),
                SinkType::MessageBased => CharacterDelimitedEncoder::new(b',').into(),
            },
            (None, Serializer::Avro(_) | Serializer::Native(_)) => {
                LengthDelimitedEncoder::default().into()
            }
            (None, Serializer::Gelf(_)) => {
                // Graylog/GELF always uses null byte delimiter on TCP, see
                // https://github.com/Graylog2/graylog2-server/issues/1240
                CharacterDelimitedEncoder::new(0).into()
            }
            (None, Serializer::Protobuf(_)) => {
                // Protobuf uses length-delimited messages, see:
                // https://developers.google.com/protocol-buffers/docs/techniques#streaming
                LengthDelimitedEncoder::default().into()
            }
            (
                None,
                Serializer::Cef(_)
                | Serializer::Csv(_)
                | Serializer::Logfmt(_)
                | Serializer::NativeJson(_)
                | Serializer::RawMessage(_)
                | Serializer::Text(_),
            ) => NewlineDelimitedEncoder::default().into(),
        };

        Ok((framer, serializer))
    }
}

/// The way a sink processes outgoing events.
pub enum SinkType {
    /// Events are sent in a continuous stream.
    StreamBased,
    /// Events are sent in a batch as a message.
    MessageBased,
}

impl<F, S> From<(Option<F>, S)> for EncodingConfigWithFraming
where
    F: Into<FramingConfig>,
    S: Into<SerializerConfig>,
{
    fn from((framing, encoding): (Option<F>, S)) -> Self {
        Self {
            framing: framing.map(Into::into),
            encoding: encoding.into().into(),
        }
    }
}

