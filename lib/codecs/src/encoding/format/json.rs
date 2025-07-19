use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use vector_config_macros::configurable_component;
use vector_core::{config::DataType, event::Event, schema};

use crate::MetricTagValues;

/// Config used to build a `JsonSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct JsonSerializerConfig {
    /// Controls how metric tag values are encoded.
    ///
    /// When set to `single`, only the last non-bare value of tags are displayed with the
    /// metric.  When set to `full`, all metric tags are exposed as separate assignments.
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    pub metric_tag_values: MetricTagValues,

    /// Options for the JsonSerializer.
    #[serde(default, rename = "json")]
    pub options: JsonSerializerOptions,
}

/// Options for the JsonSerializer.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct JsonSerializerOptions {
    /// Whether to use pretty JSON formatting.
    #[serde(default)]
    pub pretty: bool,
}

impl JsonSerializerConfig {
    /// Creates a new `JsonSerializerConfig`.
    pub const fn new(metric_tag_values: MetricTagValues, options: JsonSerializerOptions) -> Self {
        Self {
            metric_tag_values,
            options,
        }
    }

    /// Build the `JsonSerializer` from this configuration.
    pub fn build(&self) -> JsonSerializer {
        JsonSerializer::new(self.metric_tag_values, self.options.clone())
    }

    /// The data type of events that are accepted by `JsonSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::all_bits()
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // While technically we support `Value` variants that can't be losslessly serialized to
        // JSON, we don't want to enforce that limitation to users yet.
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the JSON format.
#[derive(Debug, Clone)]
pub struct JsonSerializer {
    metric_tag_values: MetricTagValues,
    options: JsonSerializerOptions,
}

impl JsonSerializer {
    /// Creates a new `JsonSerializer`.
    pub const fn new(metric_tag_values: MetricTagValues, options: JsonSerializerOptions) -> Self {
        Self {
            metric_tag_values,
            options,
        }
    }

    /// Encode event and represent it as JSON value.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, vector_common::Error> {
        match event {
            Event::Log(log) => serde_json::to_value(&log),
            Event::Metric(metric) => serde_json::to_value(&metric),
            Event::Trace(trace) => serde_json::to_value(&trace),
        }
        .map_err(|e| e.to_string().into())
    }
}

impl Encoder<Event> for JsonSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let writer = buffer.writer();
        if self.options.pretty {
            match event {
                Event::Log(log) => serde_json::to_writer_pretty(writer, &log),
                Event::Metric(mut metric) => {
                    if self.metric_tag_values == MetricTagValues::Single {
                        metric.reduce_tags_to_single();
                    }
                    serde_json::to_writer_pretty(writer, &metric)
                }
                Event::Trace(trace) => serde_json::to_writer_pretty(writer, &trace),
            }
        } else {
            match event {
                Event::Log(log) => serde_json::to_writer(writer, &log),
                Event::Metric(mut metric) => {
                    if self.metric_tag_values == MetricTagValues::Single {
                        metric.reduce_tags_to_single();
                    }
                    serde_json::to_writer(writer, &metric)
                }
                Event::Trace(trace) => serde_json::to_writer(writer, &trace),
            }
        }
        .map_err(Into::into)
    }
}

