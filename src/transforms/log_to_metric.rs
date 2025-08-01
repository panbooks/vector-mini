use std::sync::Arc;
use std::{collections::HashMap, num::ParseFloatError};

use chrono::Utc;
use indexmap::IndexMap;
use vector_lib::configurable::configurable_component;
use vector_lib::event::LogEvent;
use vector_lib::{
    config::LogNamespace,
    event::DatadogMetricOriginMetadata,
    event::{
        metric::Sample,
        metric::{Bucket, Quantile},
    },
};
use vrl::path::{parse_target_path, PathParseError};
use vrl::{event_path, path};

use crate::config::schema::Definition;
use crate::transforms::log_to_metric::TransformError::PathNotFound;
use crate::{
    common::expansion::pair_expansion,
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{
        metric::{Metric, MetricKind, MetricTags, MetricValue, StatisticKind, TagValue},
        Event, Value,
    },
    internal_events::{
        LogToMetricFieldNullError, LogToMetricParseFloatError,
        MetricMetadataInvalidFieldValueError, MetricMetadataMetricDetailsNotFoundError,
        MetricMetadataParseError, ParserMissingFieldError, DROP_EVENT,
    },
    schema,
    template::{Template, TemplateRenderingError},
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

const ORIGIN_SERVICE_VALUE: u32 = 3;

/// Configuration for the `log_to_metric` transform.
#[configurable_component(transform("log_to_metric", "Convert log events to metric events."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct LogToMetricConfig {
    /// A list of metrics to generate.
    pub metrics: Vec<MetricConfig>,
    /// Setting this flag changes the behavior of this transformation.<br />
    /// <p>Notably the `metrics` field will be ignored.</p>
    /// <p>All incoming events will be processed and if possible they will be converted to log events.
    /// Otherwise, only items specified in the 'metrics' field will be processed.</p>
    /// <pre class="chroma"><code class="language-toml" data-lang="toml">use serde_json::json;
    /// let json_event = json!({
    ///     "counter": {
    ///         "value": 10.0
    ///     },
    ///     "kind": "incremental",
    ///     "name": "test.transform.counter",
    ///     "tags": {
    ///         "env": "test_env",
    ///         "host": "localhost"
    ///     }
    /// });
    /// </code></pre>
    ///
    /// This is an example JSON representation of a counter with the following properties:
    ///
    /// - `counter`: An object with a single property `value` representing the counter value, in this case, `10.0`).
    /// - `kind`: A string indicating the kind of counter, in this case, "incremental".
    /// - `name`: A string representing the name of the counter, here set to "test.transform.counter".
    /// - `tags`: An object containing additional tags such as "env" and "host".
    ///
    /// Objects that can be processed include counter, histogram, gauge, set and summary.
    pub all_metrics: Option<bool>,
}

/// Specification of a counter derived from a log event.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct CounterConfig {
    /// Increments the counter by the value in `field`, instead of only by `1`.
    #[serde(default = "default_increment_by_value")]
    pub increment_by_value: bool,

    #[configurable(derived)]
    #[serde(default = "default_kind")]
    pub kind: MetricKind,
}

/// Specification of a metric derived from a log event.
// TODO: While we're resolving the schema for this enum somewhat reasonably (in
// `generate-components-docs.rb`), we have a problem where an overlapping field (overlap between two
// or more of the subschemas) takes the details of the last subschema to be iterated over that
// contains that field, such that, for example, the `Summary` variant below is overriding the
// description for almost all of the fields because they're shared across all of the variants.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct MetricConfig {
    /// Name of the field in the event to generate the metric.
    pub field: Template,

    /// Overrides the name of the counter.
    ///
    /// If not specified, `field` is used as the name of the metric.
    pub name: Option<Template>,

    /// Sets the namespace for the metric.
    pub namespace: Option<Template>,

    /// Tags to apply to the metric.
    ///
    /// Both keys and values can be templated, allowing you to attach dynamic tags to events.
    ///
    #[configurable(metadata(docs::additional_props_description = "A metric tag."))]
    pub tags: Option<IndexMap<Template, TagConfig>>,

    #[configurable(derived)]
    #[serde(flatten)]
    pub metric: MetricTypeConfig,
}

/// Specification of the value of a created tag.
///
/// This may be a single value, a `null` for a bare tag, or an array of either.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(untagged)]
pub enum TagConfig {
    /// A single tag value.
    Plain(Option<Template>),

    /// An array of values to give to the same tag name.
    Multi(Vec<Option<Template>>),
}

/// Specification of the type of an individual metric, and any associated data.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of metric to create."))]
pub enum MetricTypeConfig {
    /// A counter.
    Counter(CounterConfig),

    /// A histogram.
    Histogram,

    /// A gauge.
    Gauge,

    /// A set.
    Set,

    /// A summary.
    Summary,
}

impl MetricConfig {
    fn field(&self) -> &str {
        self.field.get_ref()
    }
}

const fn default_increment_by_value() -> bool {
    false
}

const fn default_kind() -> MetricKind {
    MetricKind::Incremental
}

#[derive(Debug, Clone)]
pub struct LogToMetric {
    config: LogToMetricConfig,
}

impl GenerateConfig for LogToMetricConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            metrics: vec![MetricConfig {
                field: "field_name".try_into().expect("Fixed template"),
                name: None,
                namespace: None,
                tags: None,
                metric: MetricTypeConfig::Counter(CounterConfig {
                    increment_by_value: false,
                    kind: MetricKind::Incremental,
                }),
            }],
            all_metrics: Some(true),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "log_to_metric")]
impl TransformConfig for LogToMetricConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(LogToMetric::new(self.clone())))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: vector_lib::enrichment::TableRegistry,
        _: &[(OutputId, schema::Definition)],
        _: LogNamespace,
    ) -> Vec<TransformOutput> {
        // Converting the log to a metric means we lose all incoming `Definition`s.
        vec![TransformOutput::new(DataType::Metric, HashMap::new())]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

impl LogToMetric {
    pub const fn new(config: LogToMetricConfig) -> Self {
        LogToMetric { config }
    }
}

/// Kinds of TranformError for Parsing
#[configurable_component]
#[derive(Clone, Debug)]
pub enum TransformParseErrorKind {
    ///  Error when Parsing a Float
    FloatError,
    ///  Error when Parsing an Int
    IntError,
    /// Errors when Parsing Arrays
    ArrayError,
}

impl std::fmt::Display for TransformParseErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

enum TransformError {
    PathNotFound {
        path: String,
    },
    PathNull {
        path: String,
    },
    MetricDetailsNotFound,
    MetricValueError {
        path: String,
        path_value: String,
    },
    ParseError {
        path: String,
        kind: TransformParseErrorKind,
    },
    ParseFloatError {
        path: String,
        error: ParseFloatError,
    },
    TemplateRenderingError(TemplateRenderingError),
    PairExpansionError,
}

fn render_template(template: &Template, event: &Event) -> Result<String, TransformError> {
    template
        .render_string(event)
        .map_err(TransformError::TemplateRenderingError)
}

fn render_tags(
    tags: &Option<IndexMap<Template, TagConfig>>,
    event: &Event,
) -> Result<Option<MetricTags>, TransformError> {
    let mut static_tags: HashMap<String, String> = HashMap::new();
    let mut dynamic_tags: HashMap<String, String> = HashMap::new();
    Ok(match tags {
        None => None,
        Some(tags) => {
            let mut result = MetricTags::default();
            for (name, config) in tags {
                match config {
                    TagConfig::Plain(template) => {
                        render_tag_into(
                            event,
                            name,
                            template.as_ref(),
                            &mut result,
                            &mut static_tags,
                            &mut dynamic_tags,
                        )?;
                    }
                    TagConfig::Multi(vec) => {
                        for template in vec {
                            render_tag_into(
                                event,
                                name,
                                template.as_ref(),
                                &mut result,
                                &mut static_tags,
                                &mut dynamic_tags,
                            )?;
                        }
                    }
                }
            }
            for (k, v) in static_tags {
                if let Some(discarded_v) = dynamic_tags.insert(k.clone(), v.clone()) {
                    warn!(
                        "Static tags overrides dynamic tags. \
                key: {}, value: {:?}, discarded value: {:?}",
                        k, v, discarded_v
                    );
                };
            }
            result.as_option()
        }
    })
}

fn render_tag_into(
    event: &Event,
    key_template: &Template,
    value_template: Option<&Template>,
    result: &mut MetricTags,
    static_tags: &mut HashMap<String, String>,
    dynamic_tags: &mut HashMap<String, String>,
) -> Result<(), TransformError> {
    let key_s = match render_template(key_template, event) {
        Ok(key_s) => key_s,
        Err(TransformError::TemplateRenderingError(err)) => {
            emit!(crate::internal_events::TemplateRenderingError {
                error: err,
                drop_event: false,
                field: Some(key_template.get_ref()),
            });
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    match value_template {
        None => {
            result.insert(key_s, TagValue::Bare);
        }
        Some(template) => match render_template(template, event) {
            Ok(value_s) => {
                let expanded_pairs = pair_expansion(&key_s, &value_s, static_tags, dynamic_tags)
                    .map_err(|_| TransformError::PairExpansionError)?;
                result.extend(expanded_pairs);
            }
            Err(TransformError::TemplateRenderingError(value_error)) => {
                emit!(crate::internal_events::TemplateRenderingError {
                    error: value_error,
                    drop_event: false,
                    field: Some(template.get_ref()),
                });
                return Ok(());
            }
            Err(other) => return Err(other),
        },
    };
    Ok(())
}

fn to_metric_with_config(config: &MetricConfig, event: &Event) -> Result<Metric, TransformError> {
    let log = event.as_log();

    let timestamp = log
        .get_timestamp()
        .and_then(Value::as_timestamp)
        .cloned()
        .or_else(|| Some(Utc::now()));

    // Assign the OriginService for the new metric
    let metadata = event
        .metadata()
        .clone()
        .with_schema_definition(&Arc::new(Definition::any()))
        .with_origin_metadata(DatadogMetricOriginMetadata::new(
            None,
            None,
            Some(ORIGIN_SERVICE_VALUE),
        ));

    let field = parse_target_path(config.field()).map_err(|_e| PathNotFound {
        path: config.field().to_string(),
    })?;

    let value = match log.get(&field) {
        None => Err(TransformError::PathNotFound {
            path: field.to_string(),
        }),
        Some(Value::Null) => Err(TransformError::PathNull {
            path: field.to_string(),
        }),
        Some(value) => Ok(value),
    }?;

    let name = config.name.as_ref().unwrap_or(&config.field);
    let name = render_template(name, event)?;

    let namespace = config.namespace.as_ref();
    let namespace = namespace
        .map(|namespace| render_template(namespace, event))
        .transpose()?;

    let tags = render_tags(&config.tags, event)?;

    let (kind, value) = match &config.metric {
        MetricTypeConfig::Counter(counter) => {
            let value = if counter.increment_by_value {
                value.to_string_lossy().parse().map_err(|error| {
                    TransformError::ParseFloatError {
                        path: config.field.get_ref().to_owned(),
                        error,
                    }
                })?
            } else {
                1.0
            };

            (counter.kind, MetricValue::Counter { value })
        }
        MetricTypeConfig::Histogram => {
            let value = value.to_string_lossy().parse().map_err(|error| {
                TransformError::ParseFloatError {
                    path: field.to_string(),
                    error,
                }
            })?;

            (
                MetricKind::Incremental,
                MetricValue::Distribution {
                    samples: vector_lib::samples![value => 1],
                    statistic: StatisticKind::Histogram,
                },
            )
        }
        MetricTypeConfig::Summary => {
            let value = value.to_string_lossy().parse().map_err(|error| {
                TransformError::ParseFloatError {
                    path: field.to_string(),
                    error,
                }
            })?;

            (
                MetricKind::Incremental,
                MetricValue::Distribution {
                    samples: vector_lib::samples![value => 1],
                    statistic: StatisticKind::Summary,
                },
            )
        }
        MetricTypeConfig::Gauge => {
            let value = value.to_string_lossy().parse().map_err(|error| {
                TransformError::ParseFloatError {
                    path: field.to_string(),
                    error,
                }
            })?;

            (MetricKind::Absolute, MetricValue::Gauge { value })
        }
        MetricTypeConfig::Set => {
            let value = value.to_string_lossy().into_owned();

            (
                MetricKind::Incremental,
                MetricValue::Set {
                    values: std::iter::once(value).collect(),
                },
            )
        }
    };
    Ok(Metric::new_with_metadata(name, kind, value, metadata)
        .with_namespace(namespace)
        .with_tags(tags)
        .with_timestamp(timestamp))
}

fn bytes_to_str(value: &Value) -> Option<String> {
    match value {
        Value::Bytes(bytes) => std::str::from_utf8(bytes).ok().map(|s| s.to_string()),
        _ => None,
    }
}

fn try_get_string_from_log(log: &LogEvent, path: &str) -> Result<Option<String>, TransformError> {
    // TODO: update returned errors after `TransformError` is refactored.
    let maybe_value = log.parse_path_and_get_value(path).map_err(|e| match e {
        PathParseError::InvalidPathSyntax { path } => PathNotFound {
            path: path.to_string(),
        },
    })?;
    match maybe_value {
        None => Err(PathNotFound {
            path: path.to_string(),
        }),
        Some(v) => Ok(bytes_to_str(v)),
    }
}

fn get_counter_value(log: &LogEvent) -> Result<MetricValue, TransformError> {
    let counter_value = log
        .get(event_path!("counter", "value"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "counter.value".to_string(),
        })?
        .as_float()
        .ok_or_else(|| TransformError::ParseError {
            path: "counter.value".to_string(),
            kind: TransformParseErrorKind::FloatError,
        })?;

    Ok(MetricValue::Counter {
        value: *counter_value,
    })
}

fn get_gauge_value(log: &LogEvent) -> Result<MetricValue, TransformError> {
    let gauge_value = log
        .get(event_path!("gauge", "value"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "gauge.value".to_string(),
        })?
        .as_float()
        .ok_or_else(|| TransformError::ParseError {
            path: "gauge.value".to_string(),
            kind: TransformParseErrorKind::FloatError,
        })?;
    Ok(MetricValue::Gauge {
        value: *gauge_value,
    })
}

fn get_set_value(log: &LogEvent) -> Result<MetricValue, TransformError> {
    let set_values = log
        .get(event_path!("set", "values"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "set.values".to_string(),
        })?
        .as_array()
        .ok_or_else(|| TransformError::ParseError {
            path: "set.values".to_string(),
            kind: TransformParseErrorKind::ArrayError,
        })?;

    let mut values: Vec<String> = Vec::new();
    for e_value in set_values {
        let value = e_value
            .as_bytes()
            .ok_or_else(|| TransformError::ParseError {
                path: "set.values".to_string(),
                kind: TransformParseErrorKind::ArrayError,
            })?;
        values.push(String::from_utf8_lossy(value).to_string());
    }

    Ok(MetricValue::Set {
        values: values.into_iter().collect(),
    })
}

fn get_distribution_value(log: &LogEvent) -> Result<MetricValue, TransformError> {
    let event_samples = log
        .get(event_path!("distribution", "samples"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "distribution.samples".to_string(),
        })?
        .as_array()
        .ok_or_else(|| TransformError::ParseError {
            path: "distribution.samples".to_string(),
            kind: TransformParseErrorKind::ArrayError,
        })?;

    let mut samples: Vec<Sample> = Vec::new();
    for e_sample in event_samples {
        let value = e_sample
            .get(path!("value"))
            .ok_or_else(|| TransformError::PathNotFound {
                path: "value".to_string(),
            })?
            .as_float()
            .ok_or_else(|| TransformError::ParseError {
                path: "value".to_string(),
                kind: TransformParseErrorKind::FloatError,
            })?;

        let rate = e_sample
            .get(path!("rate"))
            .ok_or_else(|| TransformError::PathNotFound {
                path: "rate".to_string(),
            })?
            .as_integer()
            .ok_or_else(|| TransformError::ParseError {
                path: "rate".to_string(),
                kind: TransformParseErrorKind::IntError,
            })?;

        samples.push(Sample {
            value: *value,
            rate: rate as u32,
        });
    }

    let statistic_str = match try_get_string_from_log(log, "distribution.statistic")? {
        Some(n) => n,
        None => {
            return Err(TransformError::PathNotFound {
                path: "distribution.statistic".to_string(),
            })
        }
    };
    let statistic_kind = match statistic_str.as_str() {
        "histogram" => Ok(StatisticKind::Histogram),
        "summary" => Ok(StatisticKind::Summary),
        _ => Err(TransformError::MetricValueError {
            path: "distribution.statistic".to_string(),
            path_value: statistic_str.to_string(),
        }),
    }?;

    Ok(MetricValue::Distribution {
        samples,
        statistic: statistic_kind,
    })
}

fn get_histogram_value(log: &LogEvent) -> Result<MetricValue, TransformError> {
    let event_buckets = log
        .get(event_path!("histogram", "buckets"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "histogram.buckets".to_string(),
        })?
        .as_array()
        .ok_or_else(|| TransformError::ParseError {
            path: "histogram.buckets".to_string(),
            kind: TransformParseErrorKind::ArrayError,
        })?;

    let mut buckets: Vec<Bucket> = Vec::new();
    for e_bucket in event_buckets {
        let upper_limit = e_bucket
            .get(path!("upper_limit"))
            .ok_or_else(|| TransformError::PathNotFound {
                path: "histogram.buckets.upper_limit".to_string(),
            })?
            .as_float()
            .ok_or_else(|| TransformError::ParseError {
                path: "histogram.buckets.upper_limit".to_string(),
                kind: TransformParseErrorKind::FloatError,
            })?;

        let count = e_bucket
            .get(path!("count"))
            .ok_or_else(|| TransformError::PathNotFound {
                path: "histogram.buckets.count".to_string(),
            })?
            .as_integer()
            .ok_or_else(|| TransformError::ParseError {
                path: "histogram.buckets.count".to_string(),
                kind: TransformParseErrorKind::IntError,
            })?;

        buckets.push(Bucket {
            upper_limit: *upper_limit,
            count: count as u64,
        });
    }

    let count = log
        .get(event_path!("histogram", "count"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "histogram.count".to_string(),
        })?
        .as_integer()
        .ok_or_else(|| TransformError::ParseError {
            path: "histogram.count".to_string(),
            kind: TransformParseErrorKind::IntError,
        })?;

    let sum = log
        .get(event_path!("histogram", "sum"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "histogram.sum".to_string(),
        })?
        .as_float()
        .ok_or_else(|| TransformError::ParseError {
            path: "histogram.sum".to_string(),
            kind: TransformParseErrorKind::FloatError,
        })?;

    Ok(MetricValue::AggregatedHistogram {
        buckets,
        count: count as u64,
        sum: *sum,
    })
}

fn get_summary_value(log: &LogEvent) -> Result<MetricValue, TransformError> {
    let event_quantiles = log
        .get(event_path!("summary", "quantiles"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "summary.quantiles".to_string(),
        })?
        .as_array()
        .ok_or_else(|| TransformError::ParseError {
            path: "summary.quantiles".to_string(),
            kind: TransformParseErrorKind::ArrayError,
        })?;

    let mut quantiles: Vec<Quantile> = Vec::new();
    for e_quantile in event_quantiles {
        let quantile = e_quantile
            .get(path!("quantile"))
            .ok_or_else(|| TransformError::PathNotFound {
                path: "summary.quantiles.quantile".to_string(),
            })?
            .as_float()
            .ok_or_else(|| TransformError::ParseError {
                path: "summary.quantiles.quantile".to_string(),
                kind: TransformParseErrorKind::FloatError,
            })?;

        let value = e_quantile
            .get(path!("value"))
            .ok_or_else(|| TransformError::PathNotFound {
                path: "summary.quantiles.value".to_string(),
            })?
            .as_float()
            .ok_or_else(|| TransformError::ParseError {
                path: "summary.quantiles.value".to_string(),
                kind: TransformParseErrorKind::FloatError,
            })?;

        quantiles.push(Quantile {
            quantile: *quantile,
            value: *value,
        })
    }

    let count = log
        .get(event_path!("summary", "count"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "summary.count".to_string(),
        })?
        .as_integer()
        .ok_or_else(|| TransformError::ParseError {
            path: "summary.count".to_string(),
            kind: TransformParseErrorKind::IntError,
        })?;

    let sum = log
        .get(event_path!("summary", "sum"))
        .ok_or_else(|| TransformError::PathNotFound {
            path: "summary.sum".to_string(),
        })?
        .as_float()
        .ok_or_else(|| TransformError::ParseError {
            path: "summary.sum".to_string(),
            kind: TransformParseErrorKind::FloatError,
        })?;

    Ok(MetricValue::AggregatedSummary {
        quantiles,
        count: count as u64,
        sum: *sum,
    })
}

fn to_metrics(event: &Event) -> Result<Metric, TransformError> {
    let log = event.as_log();
    let timestamp = log
        .get_timestamp()
        .and_then(Value::as_timestamp)
        .cloned()
        .or_else(|| Some(Utc::now()));

    let name = match try_get_string_from_log(log, "name")? {
        Some(n) => n,
        None => {
            return Err(TransformError::PathNotFound {
                path: "name".to_string(),
            })
        }
    };

    let mut tags = MetricTags::default();

    if let Some(els) = log.get(event_path!("tags")) {
        if let Some(el) = els.as_object() {
            for (key, value) in el {
                tags.insert(key.to_string(), bytes_to_str(value));
            }
        }
    }
    let tags_result = Some(tags);

    let kind_str = match try_get_string_from_log(log, "kind")? {
        Some(n) => n,
        None => {
            return Err(TransformError::PathNotFound {
                path: "kind".to_string(),
            })
        }
    };

    let kind = match kind_str.as_str() {
        "absolute" => Ok(MetricKind::Absolute),
        "incremental" => Ok(MetricKind::Incremental),
        value => Err(TransformError::MetricValueError {
            path: "kind".to_string(),
            path_value: value.to_string(),
        }),
    }?;

    let mut value: Option<MetricValue> = None;
    if let Some(root_event) = log.as_map() {
        for key in root_event.keys() {
            value = match key.as_str() {
                "gauge" => Some(get_gauge_value(log)?),
                "distribution" => Some(get_distribution_value(log)?),
                "histogram" => Some(get_histogram_value(log)?),
                "summary" => Some(get_summary_value(log)?),
                "counter" => Some(get_counter_value(log)?),
                "set" => Some(get_set_value(log)?),
                _ => None,
            };

            if value.is_some() {
                break;
            }
        }
    }

    let value = value.ok_or(TransformError::MetricDetailsNotFound)?;

    let mut metric = Metric::new_with_metadata(name, kind, value, log.metadata().clone())
        .with_tags(tags_result)
        .with_timestamp(timestamp);

    if let Ok(namespace) = try_get_string_from_log(log, "namespace") {
        metric = metric.with_namespace(namespace);
    }

    Ok(metric)
}

impl FunctionTransform for LogToMetric {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        // Metrics are "all or none" for a specific log. If a single fails, none are produced.
        let mut buffer = Vec::with_capacity(self.config.metrics.len());
        if self
            .config
            .all_metrics
            .is_some_and(|all_metrics| all_metrics)
        {
            match to_metrics(&event) {
                Ok(metric) => {
                    output.push(Event::Metric(metric));
                }
                Err(err) => {
                    match err {
                        TransformError::MetricValueError { path, path_value } => {
                            emit!(MetricMetadataInvalidFieldValueError {
                                field: path.as_ref(),
                                field_value: path_value.as_ref()
                            })
                        }
                        TransformError::PathNotFound { path } => {
                            emit!(ParserMissingFieldError::<DROP_EVENT> {
                                field: path.as_ref()
                            })
                        }
                        TransformError::ParseError { path, kind } => {
                            emit!(MetricMetadataParseError {
                                field: path.as_ref(),
                                kind: &kind.to_string(),
                            })
                        }
                        TransformError::MetricDetailsNotFound => {
                            emit!(MetricMetadataMetricDetailsNotFoundError {})
                        }
                        _ => {}
                    };
                }
            }
        } else {
            for config in self.config.metrics.iter() {
                match to_metric_with_config(config, &event) {
                    Ok(metric) => {
                        buffer.push(Event::Metric(metric));
                    }
                    Err(err) => {
                        match err {
                            TransformError::PathNull { path } => {
                                emit!(LogToMetricFieldNullError {
                                    field: path.as_ref()
                                })
                            }
                            TransformError::PathNotFound { path } => {
                                emit!(ParserMissingFieldError::<DROP_EVENT> {
                                    field: path.as_ref()
                                })
                            }
                            TransformError::ParseFloatError { path, error } => {
                                emit!(LogToMetricParseFloatError {
                                    field: path.as_ref(),
                                    error
                                })
                            }
                            TransformError::TemplateRenderingError(error) => {
                                emit!(crate::internal_events::TemplateRenderingError {
                                    error,
                                    drop_event: true,
                                    field: None,
                                })
                            }
                            _ => {}
                        };
                        // early return to prevent the partial buffer from being sent
                        return;
                    }
                }
            }
        }

        // Metric generation was successful, publish them all.
        for event in buffer {
            output.push(event);
        }
    }
}
