use std::{collections::HashMap, future::ready, task::Poll};

use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, stream, SinkExt};
use tower::Service;
use vector_lib::configurable::configurable_component;
use vector_lib::{
    event::metric::{MetricSketch, MetricTags, Quantile},
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
};

use crate::{
    config::{AcknowledgementsConfig, Input, SinkConfig, SinkContext},
    event::{
        metric::{Metric, MetricValue, Sample, StatisticKind},
        Event, KeyString,
    },
    http::HttpClient,
    internal_events::InfluxdbEncodingError,
    sinks::{
        influxdb::{
            encode_timestamp, healthcheck, influx_line_protocol, influxdb_settings, Field,
            InfluxDb1Settings, InfluxDb2Settings, ProtocolVersion,
        },
        util::{
            buffer::metrics::{MetricNormalize, MetricNormalizer, MetricSet, MetricsBuffer},
            encode_namespace,
            http::{HttpBatchService, HttpRetryLogic},
            statistic::{validate_quantiles, DistributionStatistic},
            BatchConfig, EncodedEvent, SinkBatchSettings, TowerRequestConfig,
        },
        Healthcheck, VectorSink,
    },
    tls::{TlsConfig, TlsSettings},
};

#[derive(Clone)]
struct InfluxDbSvc {
    config: InfluxDbConfig,
    protocol_version: ProtocolVersion,
    inner: HttpBatchService<BoxFuture<'static, crate::Result<hyper::Request<Bytes>>>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct InfluxDbDefaultBatchSettings;

impl SinkBatchSettings for InfluxDbDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(20);
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Configuration for the `influxdb_metrics` sink.
#[configurable_component(sink("influxdb_metrics", "Deliver metric event data to InfluxDB."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct InfluxDbConfig {
    /// Sets the default namespace for any metrics sent.
    ///
    /// This namespace is only used if a metric has no existing namespace. When a namespace is
    /// present, it is used as a prefix to the metric name, and separated with a period (`.`).
    #[serde(alias = "namespace")]
    #[configurable(metadata(docs::examples = "service"))]
    pub default_namespace: Option<String>,

    /// The endpoint to send data to.
    ///
    /// This should be a full HTTP URI, including the scheme, host, and port.
    #[configurable(metadata(docs::examples = "http://localhost:8086/"))]
    pub endpoint: String,

    #[serde(flatten)]
    pub influxdb1_settings: Option<InfluxDb1Settings>,

    #[serde(flatten)]
    pub influxdb2_settings: Option<InfluxDb2Settings>,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<InfluxDbDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    /// A map of additional tags, in the key/value pair format, to add to each measurement.
    #[configurable(metadata(docs::additional_props_description = "A tag key/value pair."))]
    #[configurable(metadata(docs::examples = "example_tags()"))]
    pub tags: Option<HashMap<String, String>>,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    /// The list of quantiles to calculate when sending distribution metrics.
    #[serde(default = "default_summary_quantiles")]
    pub quantiles: Vec<f64>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

pub fn default_summary_quantiles() -> Vec<f64> {
    vec![0.5, 0.75, 0.9, 0.95, 0.99]
}

pub fn example_tags() -> HashMap<String, String> {
    HashMap::from([("region".to_string(), "us-west-1".to_string())])
}

impl_generate_config_from_default!(InfluxDbConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "influxdb_metrics")]
impl SinkConfig for InfluxDbConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;
        let client = HttpClient::new(tls_settings, cx.proxy())?;
        let healthcheck = healthcheck(
            self.clone().endpoint,
            self.clone().influxdb1_settings,
            self.clone().influxdb2_settings,
            client.clone(),
        )?;
        validate_quantiles(&self.quantiles)?;
        let sink = InfluxDbSvc::new(self.clone(), client)?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl InfluxDbSvc {
    pub fn new(config: InfluxDbConfig, client: HttpClient) -> crate::Result<VectorSink> {
        let settings = influxdb_settings(
            config.influxdb1_settings.clone(),
            config.influxdb2_settings.clone(),
        )?;

        let endpoint = config.endpoint.clone();
        let token = settings.token();
        let protocol_version = settings.protocol_version();

        let batch = config.batch.into_batch_settings()?;
        let request = config.request.into_settings();

        let uri = settings.write_uri(endpoint)?;

        let http_service = HttpBatchService::new(client, create_build_request(uri, token.inner()));

        let influxdb_http_service = InfluxDbSvc {
            config,
            protocol_version,
            inner: http_service,
        };
        let mut normalizer = MetricNormalizer::<InfluxMetricNormalize>::default();

        let sink = request
            .batch_sink(
                HttpRetryLogic,
                influxdb_http_service,
                MetricsBuffer::new(batch.size),
                batch.timeout,
            )
            .with_flat_map(move |event: Event| {
                stream::iter({
                    let byte_size = event.size_of();
                    let json_size = event.estimated_json_encoded_size_of();

                    normalizer
                        .normalize(event.into_metric())
                        .map(|metric| Ok(EncodedEvent::new(metric, byte_size, json_size)))
                })
            })
            .sink_map_err(|error| error!(message = "Fatal influxdb sink error.", %error));

        #[allow(deprecated)]
        Ok(VectorSink::from_event_sink(sink))
    }
}

impl Service<Vec<Metric>> for InfluxDbSvc {
    type Response = http::Response<Bytes>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    // Emission of Error internal event is handled upstream by the caller
    fn poll_ready(&mut self, cx: &mut std::task::Context) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    // Emission of Error internal event is handled upstream by the caller
    fn call(&mut self, items: Vec<Metric>) -> Self::Future {
        let input = encode_events(
            self.protocol_version,
            items,
            self.config.default_namespace.as_deref(),
            self.config.tags.as_ref(),
            &self.config.quantiles,
        );
        let body = input.freeze();

        self.inner.call(body)
    }
}

fn create_build_request(
    uri: http::Uri,
    token: &str,
) -> impl Fn(Bytes) -> BoxFuture<'static, crate::Result<hyper::Request<Bytes>>>
       + Sync
       + Send
       + 'static
       + use<> {
    let auth = format!("Token {token}");
    move |body| {
        Box::pin(ready(
            hyper::Request::post(uri.clone())
                .header("Content-Type", "text/plain")
                .header("Authorization", auth.clone())
                .body(body)
                .map_err(Into::into),
        ))
    }
}

fn merge_tags(event: &Metric, tags: Option<&HashMap<String, String>>) -> Option<MetricTags> {
    match (event.tags().cloned(), tags) {
        (Some(mut event_tags), Some(config_tags)) => {
            event_tags.extend(config_tags.iter().map(|(k, v)| (k.clone(), v.clone())));
            Some(event_tags)
        }
        (Some(event_tags), None) => Some(event_tags),
        (None, Some(config_tags)) => Some(
            config_tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        (None, None) => None,
    }
}

#[derive(Default)]
pub struct InfluxMetricNormalize;

impl MetricNormalize for InfluxMetricNormalize {
    fn normalize(&mut self, state: &mut MetricSet, metric: Metric) -> Option<Metric> {
        match (metric.kind(), &metric.value()) {
            // Counters are disaggregated. We take the previous value from the state
            // and emit the difference between previous and current as a Counter
            (_, MetricValue::Counter { .. }) => state.make_incremental(metric),
            // Convert incremental gauges into absolute ones
            (_, MetricValue::Gauge { .. }) => state.make_absolute(metric),
            // All others are left as-is
            _ => Some(metric),
        }
    }
}

fn encode_events(
    protocol_version: ProtocolVersion,
    events: Vec<Metric>,
    default_namespace: Option<&str>,
    tags: Option<&HashMap<String, String>>,
    quantiles: &[f64],
) -> BytesMut {
    let mut output = BytesMut::new();
    let count = events.len();

    for event in events.into_iter() {
        let fullname = encode_namespace(event.namespace().or(default_namespace), '.', event.name());
        let ts = encode_timestamp(event.timestamp());
        let tags = merge_tags(&event, tags);
        let (metric_type, fields) = get_type_and_fields(event.value(), quantiles);

        let mut unwrapped_tags = tags.unwrap_or_default();
        unwrapped_tags.replace("metric_type".to_owned(), metric_type.to_owned());

        if let Err(error_message) = influx_line_protocol(
            protocol_version,
            &fullname,
            Some(unwrapped_tags),
            fields,
            ts,
            &mut output,
        ) {
            emit!(InfluxdbEncodingError {
                error_message,
                count,
            });
        };
    }

    // remove last '\n'
    if !output.is_empty() {
        output.truncate(output.len() - 1);
    }
    output
}

fn get_type_and_fields(
    value: &MetricValue,
    quantiles: &[f64],
) -> (&'static str, Option<HashMap<KeyString, Field>>) {
    match value {
        MetricValue::Counter { value } => ("counter", Some(to_fields(*value))),
        MetricValue::Gauge { value } => ("gauge", Some(to_fields(*value))),
        MetricValue::Set { values } => ("set", Some(to_fields(values.len() as f64))),
        MetricValue::AggregatedHistogram {
            buckets,
            count,
            sum,
        } => {
            let mut fields: HashMap<KeyString, Field> = buckets
                .iter()
                .map(|sample| {
                    (
                        format!("bucket_{}", sample.upper_limit).into(),
                        Field::UnsignedInt(sample.count),
                    )
                })
                .collect();
            fields.insert("count".into(), Field::UnsignedInt(*count));
            fields.insert("sum".into(), Field::Float(*sum));

            ("histogram", Some(fields))
        }
        MetricValue::AggregatedSummary {
            quantiles,
            count,
            sum,
        } => {
            let mut fields: HashMap<KeyString, Field> = quantiles
                .iter()
                .map(|quantile| {
                    (
                        format!("quantile_{}", quantile.quantile).into(),
                        Field::Float(quantile.value),
                    )
                })
                .collect();
            fields.insert("count".into(), Field::UnsignedInt(*count));
            fields.insert("sum".into(), Field::Float(*sum));

            ("summary", Some(fields))
        }
        MetricValue::Distribution { samples, statistic } => {
            let quantiles = match statistic {
                StatisticKind::Histogram => &[0.95] as &[_],
                StatisticKind::Summary => quantiles,
            };
            let fields = encode_distribution(samples, quantiles);
            ("distribution", fields)
        }
        MetricValue::Sketch { sketch } => match sketch {
            MetricSketch::AgentDDSketch(ddsketch) => {
                // Hard-coded quantiles because InfluxDB can't natively do anything useful with the
                // actual bins.
                let mut fields = [0.5, 0.75, 0.9, 0.99]
                    .iter()
                    .map(|q| {
                        let quantile = Quantile {
                            quantile: *q,
                            value: ddsketch.quantile(*q).unwrap_or(0.0),
                        };
                        (
                            quantile.to_percentile_string().into(),
                            Field::Float(quantile.value),
                        )
                    })
                    .collect::<HashMap<KeyString, _>>();
                fields.insert(
                    "count".into(),
                    Field::UnsignedInt(u64::from(ddsketch.count())),
                );
                fields.insert(
                    "min".into(),
                    Field::Float(ddsketch.min().unwrap_or(f64::MAX)),
                );
                fields.insert(
                    "max".into(),
                    Field::Float(ddsketch.max().unwrap_or(f64::MIN)),
                );
                fields.insert("sum".into(), Field::Float(ddsketch.sum().unwrap_or(0.0)));
                fields.insert("avg".into(), Field::Float(ddsketch.avg().unwrap_or(0.0)));

                ("sketch", Some(fields))
            }
        },
    }
}

fn encode_distribution(samples: &[Sample], quantiles: &[f64]) -> Option<HashMap<KeyString, Field>> {
    let statistic = DistributionStatistic::from_samples(samples, quantiles)?;

    Some(
        [
            ("min".into(), Field::Float(statistic.min)),
            ("max".into(), Field::Float(statistic.max)),
            ("median".into(), Field::Float(statistic.median)),
            ("avg".into(), Field::Float(statistic.avg)),
            ("sum".into(), Field::Float(statistic.sum)),
            ("count".into(), Field::Float(statistic.count as f64)),
        ]
        .into_iter()
        .chain(
            statistic
                .quantiles
                .iter()
                .map(|&(p, val)| (format!("quantile_{p:.2}").into(), Field::Float(val))),
        )
        .collect(),
    )
}

fn to_fields(value: f64) -> HashMap<KeyString, Field> {
    [("value".into(), Field::Float(value))]
        .into_iter()
        .collect()
}


#[cfg(feature = "influxdb-integration-tests")]
