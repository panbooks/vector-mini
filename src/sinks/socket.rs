use vector_lib::codecs::{
    encoding::{Framer, FramingConfig},
    TextSerializerConfig,
};
use vector_lib::configurable::configurable_component;

#[cfg(not(windows))]
use crate::sinks::util::unix::UnixSinkConfig;
use crate::{
    codecs::{Encoder, EncodingConfig, EncodingConfigWithFraming, SinkType},
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    sinks::util::{tcp::TcpSinkConfig, udp::UdpSinkConfig},
};

/// Configuration for the `socket` sink.
#[configurable_component(sink("socket", "Deliver logs to a remote socket endpoint."))]
#[derive(Clone, Debug)]
pub struct SocketSinkConfig {
    #[serde(flatten)]
    pub mode: Mode,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

/// Socket mode.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of socket to use."))]
pub enum Mode {
    /// Send over TCP.
    Tcp(TcpMode),

    /// Send over UDP.
    Udp(UdpMode),

    /// Send over a Unix domain socket (UDS), in stream mode.
    #[serde(alias = "unix")]
    UnixStream(UnixMode),

    /// Send over a Unix domain socket (UDS), in datagram mode.
    /// Unavailable on macOS, due to send(2)'s apparent non-blocking behavior,
    /// resulting in ENOBUFS errors which we currently don't handle.
    UnixDatagram(UnixMode),
}

/// TCP configuration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct TcpMode {
    #[serde(flatten)]
    config: TcpSinkConfig,

    #[serde(flatten)]
    encoding: EncodingConfigWithFraming,
}

/// UDP configuration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UdpMode {
    #[serde(flatten)]
    config: UdpSinkConfig,

    #[configurable(derived)]
    encoding: EncodingConfig,
}

/// Unix Domain Socket configuration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UnixMode {
    #[serde(flatten)]
    config: UnixSinkConfig,

    #[serde(flatten)]
    encoding: EncodingConfigWithFraming,
}

// Workaround for https://github.com/vectordotdev/vector/issues/22198.
#[cfg(windows)]
/// A Unix Domain Socket sink.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UnixSinkConfig {
    /// The Unix socket path.
    ///
    /// This should be an absolute path.
    #[configurable(metadata(docs::examples = "/path/to/socket"))]
    pub path: std::path::PathBuf,
}

impl GenerateConfig for SocketSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"address = "92.12.333.224:5000"
            mode = "tcp"
            encoding.codec = "json""#,
        )
        .unwrap()
    }
}

impl SocketSinkConfig {
    pub const fn new(mode: Mode, acknowledgements: AcknowledgementsConfig) -> Self {
        SocketSinkConfig {
            mode,
            acknowledgements,
        }
    }

    pub fn make_basic_tcp_config(
        address: String,
        acknowledgements: AcknowledgementsConfig,
    ) -> Self {
        Self::new(
            Mode::Tcp(TcpMode {
                config: TcpSinkConfig::from_address(address),
                encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            }),
            acknowledgements,
        )
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "socket")]
impl SinkConfig for SocketSinkConfig {
    async fn build(
        &self,
        _cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        match &self.mode {
            Mode::Tcp(TcpMode { config, encoding }) => {
                let transformer = encoding.transformer();
                let (framer, serializer) = encoding.build(SinkType::StreamBased)?;
                let encoder = Encoder::<Framer>::new(framer, serializer);
                config.build(transformer, encoder)
            }
            Mode::Udp(UdpMode { config, encoding }) => {
                let transformer = encoding.transformer();
                let serializer = encoding.build()?;
                let encoder = Encoder::<()>::new(serializer);
                config.build(transformer, encoder)
            }
            #[cfg(unix)]
            Mode::UnixStream(UnixMode { config, encoding }) => {
                let transformer = encoding.transformer();
                let (framer, serializer) = encoding.build(SinkType::StreamBased)?;
                let encoder = Encoder::<Framer>::new(framer, serializer);
                config.build(
                    transformer,
                    encoder,
                    super::util::service::net::UnixMode::Stream,
                )
            }
            #[allow(unused)]
            #[cfg(unix)]
            Mode::UnixDatagram(UnixMode { config, encoding }) => {
                cfg_if! {
                    if #[cfg(not(target_os = "macos"))] {
                        let transformer = encoding.transformer();
                        let (framer, serializer) = encoding.build(SinkType::StreamBased)?;
                        let encoder = Encoder::<Framer>::new(framer, serializer);
                        config.build(
                            transformer,
                            encoder,
                            super::util::service::net::UnixMode::Datagram,
                        )
                    }
                    else {
                        Err("UnixDatagram is not available on macOS platforms.".into())
                    }
                }
            }
            #[cfg(not(unix))]
            Mode::UnixStream(_) | Mode::UnixDatagram(_) => {
                Err("Unix modes are supported only on Unix platforms.".into())
            }
        }
    }

    fn input(&self) -> Input {
        let encoder_input_type = match &self.mode {
            Mode::Tcp(TcpMode { encoding, .. }) => encoding.config().1.input_type(),
            Mode::Udp(UdpMode { encoding, .. }) => encoding.config().input_type(),
            Mode::UnixStream(UnixMode { encoding, .. }) => encoding.config().1.input_type(),
            Mode::UnixDatagram(UnixMode { encoding, .. }) => encoding.config().1.input_type(),
        };
        Input::new(encoder_input_type)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

