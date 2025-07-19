//! Shared authentication config between components that use HTTP.
use std::{collections::HashMap, fmt, net::SocketAddr};

use bytes::Bytes;
use headers::{authorization::Credentials, Authorization};
use http::{header::AUTHORIZATION, HeaderMap, HeaderValue, StatusCode};
use serde::{
    de::{Error, MapAccess, Visitor},
    Deserialize,
};
use vector_config::configurable_component;
use vector_lib::{
    compile_vrl,
    event::{Event, LogEvent, VrlTarget},
    sensitive_string::SensitiveString,
    TimeZone,
};
use vrl::{
    compiler::{runtime::Runtime, CompilationResult, CompileConfig, Program},
    core::Value,
    diagnostic::Formatter,
    prelude::TypeState,
    value::{KeyString, ObjectMap},
};

use super::ErrorMessage;

/// Configuration of the authentication strategy for server mode sinks and sources.
///
/// Use the HTTP authentication with HTTPS only. The authentication credentials are passed as an
/// HTTP header without any additional encryption beyond what is provided by the transport itself.
#[configurable_component(no_deser)]
#[derive(Clone, Debug, Eq, PartialEq)]
#[configurable(metadata(docs::enum_tag_description = "The authentication strategy to use."))]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum HttpServerAuthConfig {
    /// Basic authentication.
    ///
    /// The username and password are concatenated and encoded using [base64][base64].
    ///
    /// [base64]: https://en.wikipedia.org/wiki/Base64
    Basic {
        /// The basic authentication username.
        #[configurable(metadata(docs::examples = "${USERNAME}"))]
        #[configurable(metadata(docs::examples = "username"))]
        username: String,

        /// The basic authentication password.
        #[configurable(metadata(docs::examples = "${PASSWORD}"))]
        #[configurable(metadata(docs::examples = "password"))]
        password: SensitiveString,
    },

    /// Custom authentication using VRL code.
    ///
    /// Takes in request and validates it using VRL code.
    Custom {
        /// The VRL boolean expression.
        source: String,
    },
}

// Custom deserializer implementation to default `strategy` to `basic`
impl<'de> Deserialize<'de> for HttpServerAuthConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct HttpServerAuthConfigVisitor;

        const FIELD_KEYS: [&str; 4] = ["strategy", "username", "password", "source"];

        impl<'de> Visitor<'de> for HttpServerAuthConfigVisitor {
            type Value = HttpServerAuthConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid authentication strategy (basic or custom)")
            }

            fn visit_map<A>(self, mut map: A) -> Result<HttpServerAuthConfig, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields: HashMap<&str, String> = HashMap::default();

                while let Some(key) = map.next_key::<String>()? {
                    if let Some(field_index) = FIELD_KEYS.iter().position(|k| *k == key.as_str()) {
                        if fields.contains_key(FIELD_KEYS[field_index]) {
                            return Err(Error::duplicate_field(FIELD_KEYS[field_index]));
                        }
                        fields.insert(FIELD_KEYS[field_index], map.next_value()?);
                    } else {
                        return Err(Error::unknown_field(&key, &FIELD_KEYS));
                    }
                }

                // Default to "basic" if strategy is missing
                let strategy = fields
                    .get("strategy")
                    .map(String::as_str)
                    .unwrap_or_else(|| "basic");

                match strategy {
                    "basic" => {
                        let username = fields
                            .remove("username")
                            .ok_or_else(|| Error::missing_field("username"))?;
                        let password = fields
                            .remove("password")
                            .ok_or_else(|| Error::missing_field("password"))?;
                        Ok(HttpServerAuthConfig::Basic {
                            username,
                            password: SensitiveString::from(password),
                        })
                    }
                    "custom" => {
                        let source = fields
                            .remove("source")
                            .ok_or_else(|| Error::missing_field("source"))?;
                        Ok(HttpServerAuthConfig::Custom { source })
                    }
                    _ => Err(Error::unknown_variant(strategy, &["basic", "custom"])),
                }
            }
        }

        deserializer.deserialize_map(HttpServerAuthConfigVisitor)
    }
}

impl HttpServerAuthConfig {
    /// Builds an auth matcher based on provided configuration.
    /// Used to validate configuration if needed, before passing it to the
    /// actual component for usage.
    pub fn build(
        &self,
        enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) -> crate::Result<HttpServerAuthMatcher> {
        match self {
            HttpServerAuthConfig::Basic { username, password } => {
                Ok(HttpServerAuthMatcher::AuthHeader(
                    Authorization::basic(username, password.inner()).0.encode(),
                    "Invalid username/password",
                ))
            }
            HttpServerAuthConfig::Custom { source } => {
                let functions = vrl::stdlib::all()
                    .into_iter()
                    .chain(vector_lib::enrichment::vrl_functions())
                    .chain(vector_vrl_functions::all())
                    .collect::<Vec<_>>();

                let state = TypeState::default();

                let mut config = CompileConfig::default();
                config.set_custom(enrichment_tables.clone());
                config.set_read_only();

                let CompilationResult {
                    program,
                    warnings,
                    config: _,
                } = compile_vrl(source, &functions, &state, config).map_err(|diagnostics| {
                    Formatter::new(source, diagnostics).colored().to_string()
                })?;

                if !program.final_type_info().result.is_boolean() {
                    return Err("VRL conditions must return a boolean.".into());
                }

                if !warnings.is_empty() {
                    let warnings = Formatter::new(source, warnings).colored().to_string();
                    warn!(message = "VRL compilation warning.", %warnings);
                }

                Ok(HttpServerAuthMatcher::Vrl { program })
            }
        }
    }
}

/// Built auth matcher with validated configuration
/// Can be used directly in a component to validate authentication in HTTP requests
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum HttpServerAuthMatcher {
    /// Matcher for comparing exact value of Authorization header
    AuthHeader(HeaderValue, &'static str),
    /// Matcher for running VRL script for requests, to allow for custom validation
    Vrl {
        /// Compiled VRL script
        program: Program,
    },
}

impl HttpServerAuthMatcher {
    /// Compares passed headers to the matcher
    pub fn handle_auth(
        &self,
        address: Option<&SocketAddr>,
        headers: &HeaderMap<HeaderValue>,
        path: &str,
    ) -> Result<(), ErrorMessage> {
        match self {
            HttpServerAuthMatcher::AuthHeader(expected, err_message) => {
                if let Some(header) = headers.get(AUTHORIZATION) {
                    if expected == header {
                        Ok(())
                    } else {
                        Err(ErrorMessage::new(
                            StatusCode::UNAUTHORIZED,
                            err_message.to_string(),
                        ))
                    }
                } else {
                    Err(ErrorMessage::new(
                        StatusCode::UNAUTHORIZED,
                        "No authorization header".to_owned(),
                    ))
                }
            }
            HttpServerAuthMatcher::Vrl { program } => {
                self.handle_vrl_auth(address, headers, path, program)
            }
        }
    }

    fn handle_vrl_auth(
        &self,
        address: Option<&SocketAddr>,
        headers: &HeaderMap<HeaderValue>,
        path: &str,
        program: &Program,
    ) -> Result<(), ErrorMessage> {
        let mut target = VrlTarget::new(
            Event::Log(LogEvent::from_map(
                ObjectMap::from([
                    (
                        "headers".into(),
                        Value::Object(
                            headers
                                .iter()
                                .map(|(k, v)| {
                                    (
                                        KeyString::from(k.to_string()),
                                        Value::Bytes(Bytes::copy_from_slice(v.as_bytes())),
                                    )
                                })
                                .collect::<ObjectMap>(),
                        ),
                    ),
                    (
                        "address".into(),
                        address.map_or(Value::Null, |a| Value::from(a.ip().to_string())),
                    ),
                    ("path".into(), Value::from(path.to_owned())),
                ]),
                Default::default(),
            )),
            program.info(),
            false,
        );
        let timezone = TimeZone::default();

        let result = Runtime::default().resolve(&mut target, program, &timezone);
        match result.map_err(|e| {
            warn!("Handling auth failed: {}", e);
            ErrorMessage::new(StatusCode::UNAUTHORIZED, "Auth failed".to_owned())
        })? {
            vrl::core::Value::Boolean(result) => {
                if result {
                    Ok(())
                } else {
                    Err(ErrorMessage::new(
                        StatusCode::UNAUTHORIZED,
                        "Auth failed".to_owned(),
                    ))
                }
            }
            _ => Err(ErrorMessage::new(
                StatusCode::UNAUTHORIZED,
                "Invalid return value".to_owned(),
            )),
        }
    }
}

