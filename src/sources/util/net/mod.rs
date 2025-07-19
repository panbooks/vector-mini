#[cfg(feature = "sources-utils-net-tcp")]
mod tcp;
#[cfg(feature = "sources-utils-net-udp")]
mod udp;

use std::{fmt, net::SocketAddr};

use snafu::Snafu;
use vector_lib::configurable::configurable_component;

use crate::config::{Protocol, Resource};

#[cfg(feature = "sources-utils-net-tcp")]
pub use self::tcp::{
    request_limiter::RequestLimiter, try_bind_tcp_listener, TcpNullAcker, TcpSource, TcpSourceAck,
    TcpSourceAcker, MAX_IN_FLIGHT_EVENTS_TARGET,
};
#[cfg(feature = "sources-utils-net-udp")]
pub use self::udp::try_bind_udp_socket;

#[derive(Clone, Debug, Eq, PartialEq, Snafu)]
pub enum SocketListenAddrParseError {
    #[snafu(display("Unable to parse as socket address"))]
    SocketAddrParse,
    #[snafu(display("# after \"systemd\" must be a valid integer"))]
    UsizeParse,
    #[snafu(display("Systemd indices start from 1, found 0"))]
    OneBased,
    // last case evaluated must explain all valid formats accepted
    #[snafu(display("Must be a valid IPv4/IPv6 address with port, or start with \"systemd\""))]
    UnableToParse,
}

/// The socket address to listen for connections on, or `systemd{#N}` to use the Nth socket passed by
/// systemd socket activation.
///
/// If a socket address is used, it _must_ include a port.
//
// `SocketListenAddr` is valid for any socket based source, such as `fluent` and `logstash`.
//  Socket activation is just a way for the program to get a socket for listening on.
//  Systemd can open the port, if it is a privileged number. That way the program does not
//  need to worry about dropping ports.
//  This is particularly common in non-containerized environments.
#[configurable_component]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[serde(untagged)]
#[serde(try_from = "String", into = "String")]
#[configurable(metadata(docs::examples = "0.0.0.0:9000"))]
#[configurable(metadata(docs::examples = "systemd"))]
#[configurable(metadata(docs::examples = "systemd#3"))]
pub enum SocketListenAddr {
    /// An IPv4/IPv6 address and port.
    SocketAddr(SocketAddr),

    /// A file descriptor identifier that is given from, and managed by, the socket activation feature of `systemd`.
    SystemdFd(usize),
}

impl SocketListenAddr {
    const fn as_resource(self, protocol: Protocol) -> Resource {
        match self {
            Self::SocketAddr(addr) => match protocol {
                Protocol::Tcp => Resource::tcp(addr),
                Protocol::Udp => Resource::udp(addr),
            },
            Self::SystemdFd(fd_offset) => Resource::SystemFdOffset(fd_offset),
        }
    }

    /// Gets this listen address as a `Resource`, specifically for TCP.
    #[cfg(feature = "sources-utils-net-tcp")]
    pub const fn as_tcp_resource(self) -> Resource {
        self.as_resource(Protocol::Tcp)
    }

    /// Gets this listen address as a `Resource`, specifically for UDP.
    #[cfg(feature = "sources-utils-net-udp")]
    pub const fn as_udp_resource(self) -> Resource {
        self.as_resource(Protocol::Udp)
    }
}

impl fmt::Display for SocketListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::SocketAddr(addr) => addr.fmt(f),
            Self::SystemdFd(offset) => write!(f, "systemd socket #{offset}"),
        }
    }
}

impl From<SocketAddr> for SocketListenAddr {
    fn from(addr: SocketAddr) -> Self {
        Self::SocketAddr(addr)
    }
}

impl From<usize> for SocketListenAddr {
    fn from(fd: usize) -> Self {
        Self::SystemdFd(fd)
    }
}

impl TryFrom<String> for SocketListenAddr {
    type Error = SocketListenAddrParseError;

    fn try_from(input: String) -> Result<Self, Self::Error> {
        // first attempt to parse the string into a SocketAddr directly
        match input.parse::<SocketAddr>() {
            Ok(socket_addr) => Ok(socket_addr.into()),

            // then attempt to parse a systemd file descriptor
            Err(_) => {
                let fd: usize = match input.as_str() {
                    "systemd" => Ok(0),
                    s if s.starts_with("systemd#") => s[8..]
                        .parse::<usize>()
                        .map_err(|_| Self::Error::UsizeParse)?
                        .checked_sub(1)
                        .ok_or(Self::Error::OneBased),

                    // otherwise fail
                    _ => Err(Self::Error::UnableToParse),
                }?;

                Ok(fd.into())
            }
        }
    }
}

impl From<SocketListenAddr> for String {
    fn from(addr: SocketListenAddr) -> String {
        match addr {
            SocketListenAddr::SocketAddr(addr) => addr.to_string(),
            SocketListenAddr::SystemdFd(fd) => {
                if fd == 0 {
                    "systemd".to_owned()
                } else {
                    format!("systemd#{fd}")
                }
            }
        }
    }
}

