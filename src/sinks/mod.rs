#![allow(missing_docs)]
use futures::future::BoxFuture;
use snafu::Snafu;

pub mod prelude;
pub mod util;

#[cfg(feature = "sinks-axiom")]
pub mod axiom;
#[cfg(feature = "sinks-console")]
pub mod console;
#[cfg(feature = "sinks-file")]
pub mod file;
#[cfg(feature = "sinks-http")]
pub mod http;
#[cfg(feature = "sinks-kafka")]
pub mod kafka;
#[cfg(feature = "sinks-mezmo")]
pub mod mezmo;
#[cfg(feature = "sinks-new_relic")]
pub mod new_relic;
#[cfg(feature = "sinks-webhdfs")]
pub mod opendal_common;
#[cfg(feature = "sinks-papertrail")]
pub mod papertrail;
#[cfg(all(feature = "sinks-aws_s3", feature = "aws-core"))]
pub mod s3_common;
#[cfg(feature = "sinks-socket")]
pub mod socket;
#[cfg(feature = "sinks-splunk_hec")]
pub mod splunk_hec;
#[cfg(feature = "sinks-vector")]
pub mod vector;
#[cfg(feature = "sinks-webhdfs")]
pub mod webhdfs;
#[cfg(feature = "sinks-websocket")]
pub mod websocket;
#[cfg(feature = "sinks-websocket-server")]
pub mod websocket_server;

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
