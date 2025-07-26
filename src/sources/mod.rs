#![allow(missing_docs)]
use snafu::Snafu;

#[cfg(feature = "sources-demo_logs")]
pub mod demo_logs;
#[cfg(feature = "sources-file")]
pub mod file;
#[cfg(any(
    feature = "sources-stdin",
    all(unix, feature = "sources-file_descriptor")
))]
pub mod file_descriptors;
#[cfg(feature = "sources-host_metrics")]
pub mod host_metrics;
#[cfg(feature = "sources-http_server")]
pub mod http_server;
#[cfg(feature = "sources-internal_metrics")]
pub mod internal_metrics;
#[cfg(all(unix, feature = "sources-journald"))]
pub mod journald;
#[cfg(feature = "sources-kafka")]
pub mod kafka;
#[cfg(feature = "sources-logstash")]
pub mod logstash;
#[cfg(feature = "sources-postgresql_metrics")]
pub mod postgresql_metrics;
#[cfg(feature = "sources-splunk_hec")]
pub mod splunk_hec;
#[cfg(feature = "sources-static_metrics")]
pub mod static_metrics;
#[cfg(feature = "sources-syslog")]
pub mod syslog;

pub mod util;

pub use vector_lib::source::Source;

#[allow(dead_code)] // Easier than listing out all the features that use this
/// Common build errors
#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
}
