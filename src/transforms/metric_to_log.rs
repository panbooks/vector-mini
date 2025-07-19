use chrono::Utc;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use vector_lib::codecs::MetricTagValues;
use vector_lib::config::LogNamespace;
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::{event_path, owned_value_path, path, PathPrefix};
use vector_lib::TimeZone;
use vrl::path::OwnedValuePath;
use vrl::value::kind::Collection;
use vrl::value::Kind;

use crate::config::OutputId;
use crate::{
    config::{
        log_schema, DataType, GenerateConfig, Input, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{self, Event, LogEvent, Metric},
    internal_events::MetricToLogSerializeError,
    schema::Definition,
    transforms::{FunctionTransform, OutputBuffer, Transform},
    types::Conversion,
};

/// Configuration for the `metric_to_log` transform.
#[configurable_component(transform("metric_to_log", "Convert metric events to log events."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MetricToLogConfig {
    /// Name of the tag in the metric to use for the source host.
    ///
    /// If present, the value of the tag is set on the generated log event in the `host` field,
    /// where the field key uses the [global `host_key` option][global_log_schema_host_key].
    ///
    /// [global_log_schema_host_key]: https://vector.dev/docs/reference/configuration//global-options#log_schema.host_key
    #[configurable(metadata(docs::examples = "host", docs::examples = "hostname"))]
    pub host_tag: Option<String>,

    /// The name of the time zone to apply to timestamp conversions that do not contain an explicit
    /// time zone.
    ///
    /// This overrides the [global `timezone`][global_timezone] option. The time zone name may be
    /// any name in the [TZ database][tz_database] or `local` to indicate system local time.
    ///
    /// [global_timezone]: https://vector.dev/docs/reference/configuration//global-options#timezone
    /// [tz_database]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    pub timezone: Option<TimeZone>,

    /// The namespace to use for logs. This overrides the global setting.
    #[serde(default)]
    #[configurable(metadata(docs::hidden))]
    pub log_namespace: Option<bool>,

    /// Controls how metric tag values are encoded.
    ///
    /// When set to `single`, only the last non-bare value of tags is displayed with the
    /// metric.  When set to `full`, all metric tags are exposed as separate assignments as
    /// described by [the `native_json` codec][vector_native_json].
    ///
    /// [vector_native_json]: https://github.com/vectordotdev/vector/blob/master/lib/codecs/tests/data/native_encoding/schema.cue
    #[serde(default)]
    pub metric_tag_values: MetricTagValues,
}

impl MetricToLogConfig {
    pub fn build_transform(&self, context: &TransformContext) -> MetricToLog {
        MetricToLog::new(
            self.host_tag.as_deref(),
            self.timezone.unwrap_or_else(|| context.globals.timezone()),
            context.log_namespace(self.log_namespace),
            self.metric_tag_values,
        )
    }
}

impl GenerateConfig for MetricToLogConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            host_tag: Some("host-tag".to_string()),
            timezone: None,
            log_namespace: None,
            metric_tag_values: MetricTagValues::Single,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_to_log")]
impl TransformConfig for MetricToLogConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(self.build_transform(context)))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _: vector_lib::enrichment::TableRegistry,
        input_definitions: &[(OutputId, Definition)],
        global_log_namespace: LogNamespace,
    ) -> Vec<TransformOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = schema_definition(log_namespace);

        vec![TransformOutput::new(
            DataType::Log,
            input_definitions
                .iter()
                .map(|(output, _)| (output.clone(), schema_definition.clone()))
                .collect(),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

fn schema_definition(log_namespace: LogNamespace) -> Definition {
    let mut schema_definition = Definition::default_for_namespace(&BTreeSet::from([log_namespace]))
        .with_event_field(&owned_value_path!("name"), Kind::bytes(), None)
        .with_event_field(
            &owned_value_path!("namespace"),
            Kind::bytes().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("tags"),
            Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
            None,
        )
        .with_event_field(&owned_value_path!("kind"), Kind::bytes(), None)
        .with_event_field(
            &owned_value_path!("counter"),
            Kind::object(Collection::empty().with_known("value", Kind::float())).or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("gauge"),
            Kind::object(Collection::empty().with_known("value", Kind::float())).or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("set"),
            Kind::object(Collection::empty().with_known(
                "values",
                Kind::array(Collection::empty().with_unknown(Kind::bytes())),
            ))
            .or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("distribution"),
            Kind::object(
                Collection::empty()
                    .with_known(
                        "samples",
                        Kind::array(
                            Collection::empty().with_unknown(Kind::object(
                                Collection::empty()
                                    .with_known("value", Kind::float())
                                    .with_known("rate", Kind::integer()),
                            )),
                        ),
                    )
                    .with_known("statistic", Kind::bytes()),
            )
            .or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("aggregated_histogram"),
            Kind::object(
                Collection::empty()
                    .with_known(
                        "buckets",
                        Kind::array(
                            Collection::empty().with_unknown(Kind::object(
                                Collection::empty()
                                    .with_known("upper_limit", Kind::float())
                                    .with_known("count", Kind::integer()),
                            )),
                        ),
                    )
                    .with_known("count", Kind::integer())
                    .with_known("sum", Kind::float()),
            )
            .or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("aggregated_summary"),
            Kind::object(
                Collection::empty()
                    .with_known(
                        "quantiles",
                        Kind::array(
                            Collection::empty().with_unknown(Kind::object(
                                Collection::empty()
                                    .with_known("quantile", Kind::float())
                                    .with_known("value", Kind::float()),
                            )),
                        ),
                    )
                    .with_known("count", Kind::integer())
                    .with_known("sum", Kind::float()),
            )
            .or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("sketch"),
            Kind::any().or_undefined(),
            None,
        );

    match log_namespace {
        LogNamespace::Vector => {
            // from serializing the Metric (Legacy moves it to another field)
            schema_definition = schema_definition.with_event_field(
                &owned_value_path!("timestamp"),
                Kind::bytes().or_undefined(),
                None,
            );

            // This is added as a "marker" field to determine which namespace is being used at runtime.
            // This is normally handled automatically by sources, but this is a special case.
            schema_definition = schema_definition.with_metadata_field(
                &owned_value_path!("vector"),
                Kind::object(Collection::empty()),
                None,
            );
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                schema_definition =
                    schema_definition.with_event_field(timestamp_key, Kind::timestamp(), None);
            }

            schema_definition = schema_definition.with_event_field(
                log_schema().host_key().expect("valid host key"),
                Kind::bytes().or_undefined(),
                None,
            );
        }
    }
    schema_definition
}

#[derive(Clone, Debug)]
pub struct MetricToLog {
    host_tag: Option<OwnedValuePath>,
    timezone: TimeZone,
    log_namespace: LogNamespace,
    tag_values: MetricTagValues,
}

impl MetricToLog {
    pub fn new(
        host_tag: Option<&str>,
        timezone: TimeZone,
        log_namespace: LogNamespace,
        tag_values: MetricTagValues,
    ) -> Self {
        Self {
            host_tag: host_tag.map_or(
                log_schema().host_key().cloned().map(|mut key| {
                    key.push_front_field("tags");
                    key
                }),
                |host| Some(owned_value_path!("tags", host)),
            ),
            timezone,
            log_namespace,
            tag_values,
        }
    }

    pub fn transform_one(&self, mut metric: Metric) -> Option<LogEvent> {
        if self.tag_values == MetricTagValues::Single {
            metric.reduce_tags_to_single();
        }
        serde_json::to_value(&metric)
            .map_err(|error| emit!(MetricToLogSerializeError { error }))
            .ok()
            .and_then(|value| match value {
                Value::Object(object) => {
                    let (_, _, metadata) = metric.into_parts();
                    let mut log = LogEvent::new_with_metadata(metadata);

                    // converting all fields from serde `Value` to Vector `Value`
                    for (key, value) in object {
                        log.insert(event_path!(&key), value);
                    }

                    if self.log_namespace == LogNamespace::Legacy {
                        // "Vector" namespace just leaves the `timestamp` in place.

                        let timestamp = log
                            .remove(event_path!("timestamp"))
                            .and_then(|value| {
                                Conversion::Timestamp(self.timezone)
                                    .convert(value.coerce_to_bytes())
                                    .ok()
                            })
                            .unwrap_or_else(|| event::Value::Timestamp(Utc::now()));

                        log.maybe_insert(log_schema().timestamp_key_target_path(), timestamp);

                        if let Some(host_tag) = &self.host_tag {
                            if let Some(host_value) =
                                log.remove_prune((PathPrefix::Event, host_tag), true)
                            {
                                log.maybe_insert(log_schema().host_key_target_path(), host_value);
                            }
                        }
                    }
                    if self.log_namespace == LogNamespace::Vector {
                        // Create vector metadata since this is used as a marker to see which namespace is used at runtime.
                        // This can be removed once metrics support namespacing.
                        log.insert(
                            (PathPrefix::Metadata, path!("vector")),
                            vrl::value::Value::Object(BTreeMap::new()),
                        );
                    }
                    Some(log)
                }
                _ => None,
            })
    }
}

impl FunctionTransform for MetricToLog {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let retval: Option<Event> = self
            .transform_one(event.into_metric())
            .map(|log| log.into());
        output.extend(retval.into_iter())
    }
}

