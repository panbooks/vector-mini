pub mod logs;
pub mod metrics;

use std::collections::HashMap;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use http::{StatusCode, Uri};
use snafu::{ResultExt, Snafu};
use tower::Service;
use vector_lib::configurable::configurable_component;
use vector_lib::event::{KeyString, MetricTags};
use vector_lib::sensitive_string::SensitiveString;

use crate::http::HttpClient;

pub(in crate::sinks) enum Field {
    /// string
    String(String),
    /// float
    Float(f64),
    /// unsigned integer
    /// Influx can support 64 bit integers if compiled with a flag, see:
    /// <https://github.com/influxdata/influxdb/issues/7801#issuecomment-466801839>
    UnsignedInt(u64),
    /// integer
    Int(i64),
    /// boolean
    Bool(bool),
}

#[derive(Clone, Copy, Debug)]
pub(in crate::sinks) enum ProtocolVersion {
    V1,
    V2,
}

#[derive(Debug, Snafu)]
enum ConfigError {
    #[snafu(display("InfluxDB v1 or v2 should be configured as endpoint."))]
    MissingConfiguration,
    #[snafu(display(
        "Unclear settings. Both version configured v1: {:?}, v2: {:?}.",
        v1_settings,
        v2_settings
    ))]
    BothConfiguration {
        v1_settings: InfluxDb1Settings,
        v2_settings: InfluxDb2Settings,
    },
}

/// Configuration settings for InfluxDB v0.x/v1.x.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct InfluxDb1Settings {
    /// The name of the database to write into.
    ///
    /// Only relevant when using InfluxDB v0.x/v1.x.
    #[configurable(metadata(docs::examples = "vector-database"))]
    #[configurable(metadata(docs::examples = "iot-store"))]
    database: String,

    /// The consistency level to use for writes.
    ///
    /// Only relevant when using InfluxDB v0.x/v1.x.
    #[configurable(metadata(docs::examples = "any"))]
    #[configurable(metadata(docs::examples = "one"))]
    #[configurable(metadata(docs::examples = "quorum"))]
    #[configurable(metadata(docs::examples = "all"))]
    consistency: Option<String>,

    /// The target retention policy for writes.
    ///
    /// Only relevant when using InfluxDB v0.x/v1.x.
    #[configurable(metadata(docs::examples = "autogen"))]
    #[configurable(metadata(docs::examples = "one_day_only"))]
    retention_policy_name: Option<String>,

    /// The username to authenticate with.
    ///
    /// Only relevant when using InfluxDB v0.x/v1.x.
    #[configurable(metadata(docs::examples = "todd"))]
    #[configurable(metadata(docs::examples = "vector-source"))]
    username: Option<String>,

    /// The password to authenticate with.
    ///
    /// Only relevant when using InfluxDB v0.x/v1.x.
    #[configurable(metadata(docs::examples = "${INFLUXDB_PASSWORD}"))]
    #[configurable(metadata(docs::examples = "influxdb4ever"))]
    password: Option<SensitiveString>,
}

/// Configuration settings for InfluxDB v2.x.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct InfluxDb2Settings {
    /// The name of the organization to write into.
    ///
    /// Only relevant when using InfluxDB v2.x and above.
    #[configurable(metadata(docs::examples = "my-org"))]
    #[configurable(metadata(docs::examples = "33f2cff0a28e5b63"))]
    org: String,

    /// The name of the bucket to write into.
    ///
    /// Only relevant when using InfluxDB v2.x and above.
    #[configurable(metadata(docs::examples = "vector-bucket"))]
    #[configurable(metadata(docs::examples = "4d2225e4d3d49f75"))]
    bucket: String,

    /// The [token][token_docs] to authenticate with.
    ///
    /// Only relevant when using InfluxDB v2.x and above.
    ///
    /// [token_docs]: https://v2.docs.influxdata.com/v2.0/security/tokens/
    #[configurable(metadata(docs::examples = "${INFLUXDB_TOKEN}"))]
    #[configurable(metadata(docs::examples = "ef8d5de700e7989468166c40fc8a0ccd"))]
    token: SensitiveString,
}

trait InfluxDbSettings: std::fmt::Debug {
    fn write_uri(&self, endpoint: String) -> crate::Result<Uri>;
    fn healthcheck_uri(&self, endpoint: String) -> crate::Result<Uri>;
    fn token(&self) -> SensitiveString;
    fn protocol_version(&self) -> ProtocolVersion;
}

impl InfluxDbSettings for InfluxDb1Settings {
    fn write_uri(&self, endpoint: String) -> crate::Result<Uri> {
        encode_uri(
            &endpoint,
            "write",
            &[
                ("consistency", self.consistency.clone()),
                ("db", Some(self.database.clone())),
                ("rp", self.retention_policy_name.clone()),
                ("p", self.password.as_ref().map(|v| v.inner().to_owned())),
                ("u", self.username.clone()),
                ("precision", Some("ns".to_owned())),
            ],
        )
    }

    fn healthcheck_uri(&self, endpoint: String) -> crate::Result<Uri> {
        encode_uri(&endpoint, "ping", &[])
    }

    fn token(&self) -> SensitiveString {
        SensitiveString::default()
    }

    fn protocol_version(&self) -> ProtocolVersion {
        ProtocolVersion::V1
    }
}

impl InfluxDbSettings for InfluxDb2Settings {
    fn write_uri(&self, endpoint: String) -> crate::Result<Uri> {
        encode_uri(
            &endpoint,
            "api/v2/write",
            &[
                ("org", Some(self.org.clone())),
                ("bucket", Some(self.bucket.clone())),
                ("precision", Some("ns".to_owned())),
            ],
        )
    }

    fn healthcheck_uri(&self, endpoint: String) -> crate::Result<Uri> {
        encode_uri(&endpoint, "ping", &[])
    }

    fn token(&self) -> SensitiveString {
        self.token.clone()
    }

    fn protocol_version(&self) -> ProtocolVersion {
        ProtocolVersion::V2
    }
}

fn influxdb_settings(
    influxdb1_settings: Option<InfluxDb1Settings>,
    influxdb2_settings: Option<InfluxDb2Settings>,
) -> Result<Box<dyn InfluxDbSettings>, crate::Error> {
    match (influxdb1_settings, influxdb2_settings) {
        (Some(v1_settings), Some(v2_settings)) => Err(ConfigError::BothConfiguration {
            v1_settings,
            v2_settings,
        }
        .into()),
        (None, None) => Err(ConfigError::MissingConfiguration.into()),
        (Some(settings), _) => Ok(Box::new(settings)),
        (_, Some(settings)) => Ok(Box::new(settings)),
    }
}

// V1: https://docs.influxdata.com/influxdb/v1.7/tools/api/#ping-http-endpoint
// V2: https://v2.docs.influxdata.com/v2.0/api/#operation/GetHealth
fn healthcheck(
    endpoint: String,
    influxdb1_settings: Option<InfluxDb1Settings>,
    influxdb2_settings: Option<InfluxDb2Settings>,
    mut client: HttpClient,
) -> crate::Result<super::Healthcheck> {
    let settings = influxdb_settings(influxdb1_settings, influxdb2_settings)?;

    let uri = settings.healthcheck_uri(endpoint)?;

    let request = hyper::Request::get(uri).body(hyper::Body::empty()).unwrap();

    Ok(async move {
        client
            .call(request)
            .await
            .map_err(|error| error.into())
            .and_then(|response| match response.status() {
                StatusCode::OK => Ok(()),
                StatusCode::NO_CONTENT => Ok(()),
                other => Err(super::HealthcheckError::UnexpectedStatus { status: other }.into()),
            })
    }
    .boxed())
}

// https://docs.influxdata.com/influxdb/latest/reference/syntax/line-protocol/
pub(in crate::sinks) fn influx_line_protocol(
    protocol_version: ProtocolVersion,
    measurement: &str,
    tags: Option<MetricTags>,
    fields: Option<HashMap<KeyString, Field>>,
    timestamp: i64,
    line_protocol: &mut BytesMut,
) -> Result<(), &'static str> {
    // Fields
    let unwrapped_fields = fields.unwrap_or_default();
    // LineProtocol should have a field
    if unwrapped_fields.is_empty() {
        return Err("fields must not be empty");
    }

    encode_string(measurement, line_protocol);

    // Tags are optional
    let unwrapped_tags = tags.unwrap_or_default();
    if !unwrapped_tags.is_empty() {
        line_protocol.put_u8(b',');
        encode_tags(unwrapped_tags, line_protocol);
    }
    line_protocol.put_u8(b' ');

    // Fields
    encode_fields(protocol_version, unwrapped_fields, line_protocol);
    line_protocol.put_u8(b' ');

    // Timestamp
    line_protocol.put_slice(&timestamp.to_string().into_bytes());
    line_protocol.put_u8(b'\n');
    Ok(())
}

fn encode_tags(tags: MetricTags, output: &mut BytesMut) {
    let original_len = output.len();
    // `tags` is already sorted
    for (key, value) in tags.iter_single() {
        if key.is_empty() || value.is_empty() {
            continue;
        }
        encode_string(key, output);
        output.put_u8(b'=');
        encode_string(value, output);
        output.put_u8(b',');
    }

    // remove last ','
    if output.len() > original_len {
        output.truncate(output.len() - 1);
    }
}

fn encode_fields(
    protocol_version: ProtocolVersion,
    fields: HashMap<KeyString, Field>,
    output: &mut BytesMut,
) {
    let original_len = output.len();
    for (key, value) in fields.into_iter() {
        encode_string(&key, output);
        output.put_u8(b'=');
        match value {
            Field::String(s) => {
                output.put_u8(b'"');
                for c in s.chars() {
                    if "\\\"".contains(c) {
                        output.put_u8(b'\\');
                    }
                    let mut c_buffer: [u8; 4] = [0; 4];
                    output.put_slice(c.encode_utf8(&mut c_buffer).as_bytes());
                }
                output.put_u8(b'"');
            }
            Field::Float(f) => output.put_slice(&f.to_string().into_bytes()),
            Field::UnsignedInt(i) => {
                output.put_slice(&i.to_string().into_bytes());
                let c = match protocol_version {
                    ProtocolVersion::V1 => 'i',
                    ProtocolVersion::V2 => 'u',
                };
                let mut c_buffer: [u8; 4] = [0; 4];
                output.put_slice(c.encode_utf8(&mut c_buffer).as_bytes());
            }
            Field::Int(i) => {
                output.put_slice(&i.to_string().into_bytes());
                output.put_u8(b'i');
            }
            Field::Bool(b) => {
                output.put_slice(&b.to_string().into_bytes());
            }
        };
        output.put_u8(b',');
    }

    // remove last ','
    if output.len() > original_len {
        output.truncate(output.len() - 1);
    }
}

fn encode_string(key: &str, output: &mut BytesMut) {
    for c in key.chars() {
        if "\\, =".contains(c) {
            output.put_u8(b'\\');
        }
        let mut c_buffer: [u8; 4] = [0; 4];
        output.put_slice(c.encode_utf8(&mut c_buffer).as_bytes());
    }
}

pub(in crate::sinks) fn encode_timestamp(timestamp: Option<DateTime<Utc>>) -> i64 {
    if let Some(ts) = timestamp {
        ts.timestamp_nanos_opt().unwrap()
    } else {
        encode_timestamp(Some(Utc::now()))
    }
}

pub(in crate::sinks) fn encode_uri(
    endpoint: &str,
    path: &str,
    pairs: &[(&str, Option<String>)],
) -> crate::Result<Uri> {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());

    for pair in pairs {
        if let Some(v) = &pair.1 {
            serializer.append_pair(pair.0, v);
        }
    }

    let mut url = if endpoint.ends_with('/') {
        format!("{}{}?{}", endpoint, path, serializer.finish())
    } else {
        format!("{}/{}?{}", endpoint, path, serializer.finish())
    };

    if url.ends_with('?') {
        url.pop();
    }

    Ok(url.parse::<Uri>().context(super::UriParseSnafu)?)
}



#[cfg(feature = "influxdb-integration-tests")]
