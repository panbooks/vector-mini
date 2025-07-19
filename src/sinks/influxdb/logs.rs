use std::collections::{HashMap, HashSet};

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use http::{Request, Uri};
use indoc::indoc;
use vrl::event_path;
use vrl::path::OwnedValuePath;
use vrl::value::Kind;

use vector_lib::config::log_schema;
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::lookup_v2::OptionalValuePath;
use vector_lib::lookup::PathPrefix;
use vector_lib::schema;

use super::{
    encode_timestamp, healthcheck, influx_line_protocol, influxdb_settings, Field,
    InfluxDb1Settings, InfluxDb2Settings, ProtocolVersion,
};
use crate::{
    codecs::Transformer,
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    event::{Event, KeyString, MetricTags, Value},
    http::HttpClient,
    internal_events::InfluxdbEncodingError,
    sinks::{
        util::{
            http::{BatchedHttpSink, HttpEventEncoder, HttpSink},
            BatchConfig, Buffer, Compression, SinkBatchSettings, TowerRequestConfig,
        },
        Healthcheck, VectorSink,
    },
    tls::{TlsConfig, TlsSettings},
};

#[derive(Clone, Copy, Debug, Default)]
pub struct InfluxDbLogsDefaultBatchSettings;

impl SinkBatchSettings for InfluxDbLogsDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(1_000_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Configuration for the `influxdb_logs` sink.
#[configurable_component(sink("influxdb_logs", "Deliver log event data to InfluxDB."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct InfluxDbLogsConfig {
    /// The namespace of the measurement name to use.
    ///
    /// When specified, the measurement name is `<namespace>.vector`.
    ///
    #[configurable(
        deprecated = "This field is deprecated, and `measurement` should be used instead."
    )]
    #[configurable(metadata(docs::examples = "service"))]
    pub namespace: Option<String>,

    /// The name of the InfluxDB measurement that is written to.
    #[configurable(metadata(docs::examples = "vector-logs"))]
    pub measurement: Option<String>,

    /// The endpoint to send data to.
    ///
    /// This should be a full HTTP URI, including the scheme, host, and port.
    #[configurable(metadata(docs::examples = "http://localhost:8086"))]
    pub endpoint: String,

    /// The list of names of log fields that should be added as tags to each measurement.
    ///
    /// By default Vector adds `metric_type` as well as the configured `log_schema.host_key` and
    /// `log_schema.source_type_key` options.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "field1"))]
    #[configurable(metadata(docs::examples = "parent.child_field"))]
    pub tags: Vec<KeyString>,

    #[serde(flatten)]
    pub influxdb1_settings: Option<InfluxDb1Settings>,

    #[serde(flatten)]
    pub influxdb2_settings: Option<InfluxDb2Settings>,

    #[configurable(derived)]
    #[serde(skip_serializing_if = "crate::serde::is_default", default)]
    pub encoding: Transformer,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<InfluxDbLogsDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    acknowledgements: AcknowledgementsConfig,

    // `host_key`, `message_key`, and `source_type_key` are `Option` as we want `vector generate`
    // to produce a config with these as `None`, to not accidentally override a users configured
    // `log_schema`. Generating is constrained by build-time and can't account for changes to the
    // default `log_schema`.
    /// Use this option to customize the key containing the hostname.
    ///
    /// The setting of `log_schema.host_key`, usually `host`, is used here by default.
    #[configurable(metadata(docs::examples = "hostname"))]
    pub host_key: Option<OptionalValuePath>,

    /// Use this option to customize the key containing the message.
    ///
    /// The setting of `log_schema.message_key`, usually `message`, is used here by default.
    #[configurable(metadata(docs::examples = "text"))]
    pub message_key: Option<OptionalValuePath>,

    /// Use this option to customize the key containing the source_type.
    ///
    /// The setting of `log_schema.source_type_key`, usually `source_type`, is used here by default.
    #[configurable(metadata(docs::examples = "source"))]
    pub source_type_key: Option<OptionalValuePath>,
}

#[derive(Debug)]
struct InfluxDbLogsSink {
    uri: Uri,
    token: String,
    protocol_version: ProtocolVersion,
    measurement: String,
    tags: HashSet<KeyString>,
    transformer: Transformer,
    host_key: OwnedValuePath,
    message_key: OwnedValuePath,
    source_type_key: OwnedValuePath,
}

impl GenerateConfig for InfluxDbLogsConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
            endpoint = "http://localhost:8086/"
            namespace = "my-namespace"
            tags = []
            org = "my-org"
            bucket = "my-bucket"
            token = "${INFLUXDB_TOKEN}"
        "#})
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "influxdb_logs")]
impl SinkConfig for InfluxDbLogsConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let measurement = self.get_measurement()?;
        let tags: HashSet<KeyString> = self.tags.iter().cloned().collect();

        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;
        let client = HttpClient::new(tls_settings, cx.proxy())?;
        let healthcheck = self.healthcheck(client.clone())?;

        let batch = self.batch.into_batch_settings()?;
        let request = self.request.into_settings();

        let settings = influxdb_settings(
            self.influxdb1_settings.clone(),
            self.influxdb2_settings.clone(),
        )
        .unwrap();

        let endpoint = self.endpoint.clone();
        let uri = settings.write_uri(endpoint).unwrap();

        let token = settings.token();
        let protocol_version = settings.protocol_version();

        let host_key = self
            .host_key
            .as_ref()
            .and_then(|k| k.path.clone())
            .or_else(|| log_schema().host_key().cloned())
            .expect("global log_schema.host_key to be valid path");

        let message_key = self
            .message_key
            .as_ref()
            .and_then(|k| k.path.clone())
            .or_else(|| log_schema().message_key().cloned())
            .expect("global log_schema.message_key to be valid path");

        let source_type_key = self
            .source_type_key
            .as_ref()
            .and_then(|k| k.path.clone())
            .or_else(|| log_schema().source_type_key().cloned())
            .expect("global log_schema.source_type_key to be valid path");

        let sink = InfluxDbLogsSink {
            uri,
            token: token.inner().to_owned(),
            protocol_version,
            measurement,
            tags,
            transformer: self.encoding.clone(),
            host_key,
            message_key,
            source_type_key,
        };

        let sink = BatchedHttpSink::new(
            sink,
            Buffer::new(batch.size, Compression::None),
            request,
            batch.timeout,
            client,
        )
        .sink_map_err(|error| error!(message = "Fatal influxdb_logs sink error.", %error));

        #[allow(deprecated)]
        Ok((VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        let requirements = schema::Requirement::empty()
            .optional_meaning("message", Kind::bytes())
            .optional_meaning("host", Kind::bytes())
            .optional_meaning("timestamp", Kind::timestamp());

        Input::log().with_schema_requirement(requirements)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

struct InfluxDbLogsEncoder {
    protocol_version: ProtocolVersion,
    measurement: String,
    tags: HashSet<KeyString>,
    transformer: Transformer,
    host_key: OwnedValuePath,
    message_key: OwnedValuePath,
    source_type_key: OwnedValuePath,
}

impl HttpEventEncoder<BytesMut> for InfluxDbLogsEncoder {
    fn encode_event(&mut self, event: Event) -> Option<BytesMut> {
        let mut log = event.into_log();
        // If the event isn't an object (`. = "foo"`), inserting or renaming will result in losing
        // the original value that was assigned to the root. To avoid this we intentionally rename
        // the path that points to "message" such that it has a dedicated key.
        // TODO: add a `TargetPath::is_event_root()` to conditionally rename?
        if let Some(message_path) = log.message_path().cloned().as_ref() {
            log.rename_key(message_path, (PathPrefix::Event, &self.message_key));
        }
        // Add the `host` and `source_type` to the HashSet of tags to include
        // Ensure those paths are on the event to be encoded, rather than metadata
        if let Some(host_path) = log.host_path().cloned().as_ref() {
            self.tags.replace(host_path.path.to_string().into());
            log.rename_key(host_path, (PathPrefix::Event, &self.host_key));
        }

        if let Some(source_type_path) = log.source_type_path().cloned().as_ref() {
            self.tags.replace(source_type_path.path.to_string().into());
            log.rename_key(source_type_path, (PathPrefix::Event, &self.source_type_key));
        }

        self.tags.replace("metric_type".into());
        log.insert(event_path!("metric_type"), "logs");

        // Timestamp
        let timestamp = encode_timestamp(match log.remove_timestamp() {
            Some(Value::Timestamp(ts)) => Some(ts),
            _ => None,
        });

        let log = {
            let mut event = Event::from(log);
            self.transformer.transform(&mut event);
            event.into_log()
        };

        // Tags + Fields
        let mut tags = MetricTags::default();
        let mut fields: HashMap<KeyString, Field> = HashMap::new();
        log.convert_to_fields().for_each(|(key, value)| {
            if self.tags.contains(&key[..]) {
                tags.replace(key.into(), value.to_string_lossy().into_owned());
            } else {
                fields.insert(key, to_field(value));
            }
        });

        let mut output = BytesMut::new();
        if let Err(error_message) = influx_line_protocol(
            self.protocol_version,
            &self.measurement,
            Some(tags),
            Some(fields),
            timestamp,
            &mut output,
        ) {
            emit!(InfluxdbEncodingError {
                error_message,
                count: 1
            });
            return None;
        };

        Some(output)
    }
}

impl HttpSink for InfluxDbLogsSink {
    type Input = BytesMut;
    type Output = BytesMut;
    type Encoder = InfluxDbLogsEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        InfluxDbLogsEncoder {
            protocol_version: self.protocol_version,
            measurement: self.measurement.clone(),
            tags: self.tags.clone(),
            transformer: self.transformer.clone(),
            host_key: self.host_key.clone(),
            message_key: self.message_key.clone(),
            source_type_key: self.source_type_key.clone(),
        }
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<Request<Bytes>> {
        Request::post(&self.uri)
            .header("Content-Type", "text/plain")
            .header("Authorization", format!("Token {}", &self.token))
            .body(events.freeze())
            .map_err(Into::into)
    }
}

impl InfluxDbLogsConfig {
    fn get_measurement(&self) -> Result<String, &'static str> {
        match (self.measurement.as_ref(), self.namespace.as_ref()) {
            (Some(measure), Some(_)) => {
                warn!("Option `namespace` has been superseded by `measurement`.");
                Ok(measure.clone())
            }
            (Some(measure), None) => Ok(measure.clone()),
            (None, Some(namespace)) => {
                warn!(
                    "Option `namespace` has been deprecated. Use `measurement` instead. \
                       For example, you can use `measurement=<namespace>.vector` for the \
                       same effect."
                );
                Ok(format!("{namespace}.vector"))
            }
            (None, None) => Err("The `measurement` option is required."),
        }
    }

    fn healthcheck(&self, client: HttpClient) -> crate::Result<Healthcheck> {
        let config = self.clone();

        let healthcheck = healthcheck(
            config.endpoint,
            config.influxdb1_settings,
            config.influxdb2_settings,
            client,
        )?;

        Ok(healthcheck)
    }
}

fn to_field(value: &Value) -> Field {
    match value {
        Value::Integer(num) => Field::Int(*num),
        Value::Float(num) => Field::Float(num.into_inner()),
        Value::Boolean(b) => Field::Bool(*b),
        _ => Field::String(value.to_string_lossy().into_owned()),
    }
}


#[cfg(feature = "influxdb-integration-tests")]
