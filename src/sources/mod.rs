#![allow(missing_docs)]
use snafu::Snafu;

#[cfg(feature = "sources-demo_logs")]
pub mod demo_logs;
#[cfg(feature = "sources-file")]
pub mod file;
#[cfg(feature = "sources-host_metrics")]
pub mod host_metrics;
#[cfg(feature = "sources-http_server")]
pub mod http_server;
#[cfg(feature = "sources-kafka")]
pub mod kafka;
#[cfg(feature = "sources-splunk_hec")]
pub mod splunk_hec;

pub mod util;

pub use vector_lib::source::Source;

#[allow(dead_code)] // Easier than listing out all the features that use this
/// Common build errors
#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
}
