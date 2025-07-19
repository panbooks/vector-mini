use std::os::fd::{FromRawFd as _, IntoRawFd as _, RawFd};
use std::{fs::File, io};

use super::{outputs, FileDescriptorConfig};
use vector_lib::codecs::decoding::{DeserializerConfig, FramingConfig};
use vector_lib::config::LogNamespace;
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::lookup_v2::OptionalValuePath;

use crate::{
    config::{GenerateConfig, Resource, SourceConfig, SourceContext, SourceOutput},
    serde::default_decoding,
};
/// Configuration for the `file_descriptor` source.
#[configurable_component(source("file_descriptor", "Collect logs from a file descriptor."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileDescriptorSourceConfig {
    /// The maximum buffer size, in bytes, of incoming messages.
    ///
    /// Messages larger than this are truncated.
    #[serde(default = "crate::serde::default_max_length")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    pub max_length: usize,

    /// Overrides the name of the log field used to add the current hostname to each event.
    ///
    ///
    /// By default, the [global `host_key` option](https://vector.dev/docs/reference/configuration//global-options#log_schema.host_key) is used.
    pub host_key: Option<OptionalValuePath>,

    #[configurable(derived)]
    pub framing: Option<FramingConfig>,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    pub decoding: DeserializerConfig,

    /// The file descriptor number to read from.
    #[configurable(metadata(docs::examples = 10))]
    #[configurable(metadata(docs::human_name = "File Descriptor Number"))]
    pub fd: u32,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

impl FileDescriptorConfig for FileDescriptorSourceConfig {
    fn host_key(&self) -> Option<OptionalValuePath> {
        self.host_key.clone()
    }

    fn framing(&self) -> Option<FramingConfig> {
        self.framing.clone()
    }

    fn decoding(&self) -> DeserializerConfig {
        self.decoding.clone()
    }

    fn description(&self) -> String {
        format!("file descriptor {}", self.fd)
    }
}

impl GenerateConfig for FileDescriptorSourceConfig {
    fn generate_config() -> toml::Value {
        let fd = null_fd().unwrap();
        toml::from_str(&format!(
            r#"
            fd = {fd}
            "#
        ))
        .unwrap()
    }
}

pub(crate) fn null_fd() -> crate::Result<RawFd> {
    #[cfg(unix)]
    const FILENAME: &str = "/dev/null";
    #[cfg(windows)]
    const FILENAME: &str = "C:\\NUL";
    File::open(FILENAME)
        .map_err(|error| format!("Could not open dummy file at {FILENAME:?}: {error}").into())
        .map(|file| file.into_raw_fd())
}

#[async_trait::async_trait]
#[typetag::serde(name = "file_descriptor")]
impl SourceConfig for FileDescriptorSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source> {
        let pipe = io::BufReader::new(unsafe { File::from_raw_fd(self.fd as i32) });
        let log_namespace = cx.log_namespace(self.log_namespace);

        self.source(pipe, cx.shutdown, cx.out, log_namespace)
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);

        outputs(log_namespace, &self.host_key, &self.decoding, Self::NAME)
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::Fd(self.fd)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

