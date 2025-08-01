use std::{
    collections::{hash_map::Entry, HashMap},
    pin::Pin,
    time::Duration,
};

use async_stream::stream;
use futures::{Stream, StreamExt};
use vector_lib::{config::LogNamespace, event::MetricValue};
use vector_lib::{
    configurable::configurable_component,
    event::metric::{Metric, MetricData, MetricKind, MetricSeries},
};

use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    event::{Event, EventMetadata},
    internal_events::{AggregateEventRecorded, AggregateFlushed, AggregateUpdateFailed},
    schema,
    transforms::{TaskTransform, Transform},
};

/// Configuration for the `aggregate` transform.
#[configurable_component(transform("aggregate", "Aggregate metrics passing through a topology."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// The interval between flushes, in milliseconds.
    ///
    /// During this time frame, metrics (beta) with the same series data (name, namespace, tags, and so on) are aggregated.
    #[serde(default = "default_interval_ms")]
    #[configurable(metadata(docs::human_name = "Flush Interval"))]
    pub interval_ms: u64,
    /// Function to use for aggregation.
    ///
    /// Some of the functions may only function on incremental and some only on absolute metrics.
    #[serde(default = "default_mode")]
    #[configurable(derived)]
    pub mode: AggregationMode,
}

#[configurable_component]
#[derive(Clone, Debug, Default)]
#[configurable(description = "The aggregation mode to use.")]
pub enum AggregationMode {
    /// Default mode. Sums incremental metrics and uses the latest value for absolute metrics.
    #[default]
    Auto,

    /// Sums incremental metrics, ignores absolute
    Sum,

    /// Returns the latest value for absolute metrics, ignores incremental
    Latest,

    /// Counts metrics for incremental and absolute metrics
    Count,

    /// Returns difference between latest value for absolute, ignores incremental
    Diff,

    /// Max value of absolute metric, ignores incremental
    Max,

    /// Min value of absolute metric, ignores incremental
    Min,

    /// Mean value of absolute metric, ignores incremental
    Mean,

    /// Stdev value of absolute metric, ignores incremental
    Stdev,
}

const fn default_mode() -> AggregationMode {
    AggregationMode::Auto
}

const fn default_interval_ms() -> u64 {
    10 * 1000
}

impl_generate_config_from_default!(AggregateConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "aggregate")]
impl TransformConfig for AggregateConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Aggregate::new(self).map(Transform::event_task)
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _: vector_lib::enrichment::TableRegistry,
        _: &[(OutputId, schema::Definition)],
        _: LogNamespace,
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(DataType::Metric, HashMap::new())]
    }
}

type MetricEntry = (MetricData, EventMetadata);

#[derive(Debug)]
pub struct Aggregate {
    interval: Duration,
    map: HashMap<MetricSeries, MetricEntry>,
    prev_map: HashMap<MetricSeries, MetricEntry>,
    multi_map: HashMap<MetricSeries, Vec<MetricEntry>>,
    mode: AggregationMode,
}

impl Aggregate {
    pub fn new(config: &AggregateConfig) -> crate::Result<Self> {
        Ok(Self {
            interval: Duration::from_millis(config.interval_ms),
            map: Default::default(),
            prev_map: Default::default(),
            multi_map: Default::default(),
            mode: config.mode.clone(),
        })
    }

    fn record(&mut self, event: Event) {
        let (series, data, metadata) = event.into_metric().into_parts();

        match self.mode {
            AggregationMode::Auto => match data.kind {
                MetricKind::Incremental => self.record_sum(series, data, metadata),
                MetricKind::Absolute => {
                    self.map.insert(series, (data, metadata));
                }
            },
            AggregationMode::Sum => self.record_sum(series, data, metadata),
            AggregationMode::Latest | AggregationMode::Diff => match data.kind {
                MetricKind::Incremental => (),
                MetricKind::Absolute => {
                    self.map.insert(series, (data, metadata));
                }
            },
            AggregationMode::Count => self.record_count(series, data, metadata),
            AggregationMode::Max | AggregationMode::Min => {
                self.record_comparison(series, data, metadata)
            }
            AggregationMode::Mean | AggregationMode::Stdev => match data.kind {
                MetricKind::Incremental => (),
                MetricKind::Absolute => {
                    if matches!(data.value, MetricValue::Gauge { value: _ }) {
                        match self.multi_map.entry(series) {
                            Entry::Occupied(mut entry) => {
                                let existing = entry.get_mut();
                                existing.push((data, metadata));
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(vec![(data, metadata)]);
                            }
                        }
                    }
                }
            },
        }

        emit!(AggregateEventRecorded);
    }

    fn record_count(
        &mut self,
        series: MetricSeries,
        mut data: MetricData,
        metadata: EventMetadata,
    ) {
        let mut count_data = data.clone();
        let existing = self.map.entry(series).or_insert_with(|| {
            *data.value_mut() = MetricValue::Counter { value: 0f64 };
            (data.clone(), metadata.clone())
        });
        *count_data.value_mut() = MetricValue::Counter { value: 1f64 };
        if existing.0.kind == data.kind && existing.0.update(&count_data) {
            existing.1.merge(metadata);
        } else {
            emit!(AggregateUpdateFailed);
        }
    }

    fn record_sum(&mut self, series: MetricSeries, data: MetricData, metadata: EventMetadata) {
        match data.kind {
            MetricKind::Incremental => match self.map.entry(series) {
                Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    // In order to update (add) the new and old kind's must match
                    if existing.0.kind == data.kind && existing.0.update(&data) {
                        existing.1.merge(metadata);
                    } else {
                        emit!(AggregateUpdateFailed);
                        *existing = (data, metadata);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((data, metadata));
                }
            },
            MetricKind::Absolute => {}
        }
    }

    fn record_comparison(
        &mut self,
        series: MetricSeries,
        data: MetricData,
        metadata: EventMetadata,
    ) {
        match data.kind {
            MetricKind::Incremental => (),
            MetricKind::Absolute => match self.map.entry(series) {
                Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    // In order to update (add) the new and old kind's must match
                    if existing.0.kind == data.kind {
                        if let MetricValue::Gauge {
                            value: existing_value,
                        } = existing.0.value()
                        {
                            if let MetricValue::Gauge { value: new_value } = data.value() {
                                let should_update = match self.mode {
                                    AggregationMode::Max => new_value > existing_value,
                                    AggregationMode::Min => new_value < existing_value,
                                    _ => false,
                                };
                                if should_update {
                                    *existing = (data, metadata);
                                }
                            }
                        }
                    } else {
                        emit!(AggregateUpdateFailed);
                        *existing = (data, metadata);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((data, metadata));
                }
            },
        }
    }

    fn flush_into(&mut self, output: &mut Vec<Event>) {
        let map = std::mem::take(&mut self.map);
        for (series, entry) in map.clone().into_iter() {
            let mut metric = Metric::from_parts(series, entry.0, entry.1);
            if matches!(self.mode, AggregationMode::Diff) {
                if let Some(prev_entry) = self.prev_map.get(metric.series()) {
                    if metric.data().kind == prev_entry.0.kind && !metric.subtract(&prev_entry.0) {
                        emit!(AggregateUpdateFailed);
                    }
                }
            }
            output.push(Event::Metric(metric));
        }

        let multi_map = std::mem::take(&mut self.multi_map);
        'outer: for (series, entries) in multi_map.into_iter() {
            if entries.is_empty() {
                continue;
            }

            let (mut final_sum, mut final_metadata) = entries.first().unwrap().clone();
            for (data, metadata) in entries.iter().skip(1) {
                if !final_sum.update(data) {
                    // Incompatible types, skip this metric
                    emit!(AggregateUpdateFailed);
                    continue 'outer;
                }
                final_metadata.merge(metadata.clone());
            }

            let final_mean_value = if let MetricValue::Gauge { value } = final_sum.value_mut() {
                // Entries are not empty so this is safe.
                *value /= entries.len() as f64;
                *value
            } else {
                0.0
            };

            let final_mean = final_sum.clone();
            match self.mode {
                AggregationMode::Mean => {
                    let metric = Metric::from_parts(series, final_mean, final_metadata);
                    output.push(Event::Metric(metric));
                }
                AggregationMode::Stdev => {
                    let variance = entries
                        .iter()
                        .filter_map(|(data, _)| {
                            if let MetricValue::Gauge { value } = data.value() {
                                let diff = final_mean_value - value;
                                Some(diff * diff)
                            } else {
                                None
                            }
                        })
                        .sum::<f64>()
                        / entries.len() as f64;
                    let mut final_stdev = final_mean;
                    if let MetricValue::Gauge { value } = final_stdev.value_mut() {
                        *value = variance.sqrt()
                    }
                    let metric = Metric::from_parts(series, final_stdev, final_metadata);
                    output.push(Event::Metric(metric));
                }
                _ => (),
            }
        }

        self.prev_map = map;
        emit!(AggregateFlushed);
    }
}

impl TaskTransform<Event> for Aggregate {
    fn transform(
        mut self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let mut flush_stream = tokio::time::interval(self.interval);

        Box::pin(stream! {
            let mut output = Vec::new();
            let mut done = false;
            while !done {
                tokio::select! {
                    _ = flush_stream.tick() => {
                        self.flush_into(&mut output);
                    },
                    maybe_event = input_rx.next() => {
                        match maybe_event {
                            None => {
                                self.flush_into(&mut output);
                                done = true;
                            }
                            Some(event) => self.record(event),
                        }
                    }
                };
                for event in output.drain(..) {
                    yield event;
                }
            }
        })
    }
}
