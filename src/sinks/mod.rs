#![allow(missing_docs)]
use futures::future::BoxFuture;
use snafu::Snafu;

pub mod prelude;
pub mod util;

#[cfg(feature = "sinks-axiom")]
pub mod console;
#[cfg(feature = "sinks-databend")]
pub mod file;
#[cfg(feature = "sinks-kafka")]
pub mod kafka;
pub mod papertrail;
pub mod socket;
#[cfg(feature = "sinks-splunk_hec")]

pub use vector_lib::{config::Input, sink::VectorSink};

pub type Healthcheck = BoxFuture<'static, crate::Result<()>>;

/// Common build errors
#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("Unable to resolve DNS for {:?}", address))]
    DnsFailure { address: String },
    #[snafu(display("DNS errored {}", source))]
    DnsError { source: crate::dns::DnsError },
    #[snafu(display("Socket address problem: {}", source))]
    SocketAddressError { source: std::io::Error },
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
    #[snafu(display("HTTP request build error: {}", source))]
    HTTPRequestBuilderError { source: ::http::Error },
}

/// Common healthcheck errors
#[derive(Debug, Snafu)]
pub enum HealthcheckError {
    #[snafu(display("Unexpected status: {}", status))]
    UnexpectedStatus { status: ::http::StatusCode },
}
