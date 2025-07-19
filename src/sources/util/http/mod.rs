#[cfg(feature = "sources-utils-http-encoding")]
mod encoding;
mod method;
#[cfg(feature = "sources-utils-http-prelude")]
mod prelude;

#[cfg(feature = "sources-utils-http-encoding")]
pub use encoding::decode;
pub use method::HttpMethod;
#[cfg(feature = "sources-utils-http-prelude")]
pub use prelude::HttpSource;
