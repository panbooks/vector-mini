#![deny(warnings)]

use std::{collections::BTreeMap, convert::TryFrom};

use indexmap::IndexMap;
use snafu::ResultExt;

mod line;

pub use line::ErrorKind;
use line::{Line, Metric, MetricKind};

pub const METRIC_NAME_LABEL: &str = "__name__";

#[allow(warnings)] // Ignore some clippy warnings
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));

    pub use metric_metadata::MetricType;

    impl MetricType {
        pub fn as_str(&self) -> &'static str {
            match self {
                MetricType::Counter => "counter",
                MetricType::Gauge => "gauge",
                MetricType::Histogram => "histogram",
                MetricType::Summary => "summary",
                MetricType::Gaugehistogram => "gaugehistogram",
                MetricType::Info => "info",
                MetricType::Stateset => "stateset",
                MetricType::Unknown => "unknown",
            }
        }
    }
}

#[derive(Debug, snafu::Snafu, PartialEq)]
pub enum ParserError {
    #[snafu(display("{}, line: `{}`", kind, line))]
    WithLine {
        line: String,
        #[snafu(source)]
        kind: ErrorKind,
    },
    #[snafu(display("expected \"le\" tag for histogram metric"))]
    ExpectedLeTag,
    #[snafu(display("expected \"quantile\" tag for summary metric"))]
    ExpectedQuantileTag,

    #[snafu(display("error parsing label value: {}", error))]
    ParseLabelValue {
        #[snafu(source)]
        error: ErrorKind,
    },

    #[snafu(display("expected value in range [0, {}], found: {}", max, value))]
    ValueOutOfRange { value: f64, max: u64 },

    #[snafu(display("multiple metric kinds given for metric name `{}`", name))]
    MultipleMetricKinds { name: String },
    #[snafu(display("request is missing metric name label"))]
    RequestNoNameLabel,
}

vector_common::impl_event_data_eq!(ParserError);

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct GroupKey {
    pub timestamp: Option<i64>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Default, PartialEq)]
pub struct SummaryQuantile {
    pub quantile: f64,
    pub value: f64,
}

#[derive(Debug, Default, PartialEq)]
pub struct SummaryMetric {
    pub quantiles: Vec<SummaryQuantile>,
    pub sum: f64,
    pub count: u64,
}

#[derive(Debug, Default, PartialEq, PartialOrd)]
pub struct HistogramBucket {
    pub bucket: f64,
    pub count: u64,
}

#[derive(Debug, Default, PartialEq)]
pub struct HistogramMetric {
    pub buckets: Vec<HistogramBucket>,
    pub sum: f64,
    pub count: u64,
}

#[derive(Debug, Default, PartialEq)]
pub struct SimpleMetric {
    pub value: f64,
}

type MetricMap<T> = IndexMap<GroupKey, T>;

#[derive(Debug)]
pub enum GroupKind {
    Summary(MetricMap<SummaryMetric>),
    Histogram(MetricMap<HistogramMetric>),
    Gauge(MetricMap<SimpleMetric>),
    Counter(MetricMap<SimpleMetric>),
    Untyped(MetricMap<SimpleMetric>),
}

impl GroupKind {
    fn new(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Histogram => Self::Histogram(IndexMap::default()),
            MetricKind::Summary => Self::Summary(IndexMap::default()),
            MetricKind::Counter => Self::Counter(IndexMap::default()),
            MetricKind::Gauge => Self::Gauge(IndexMap::default()),
            MetricKind::Untyped => Self::Untyped(IndexMap::default()),
        }
    }

    fn new_untyped(key: GroupKey, value: f64) -> Self {
        let mut metrics = IndexMap::default();
        metrics.insert(key, SimpleMetric { value });
        Self::Untyped(metrics)
    }

    fn matches_kind(&self, kind: MetricKind) -> bool {
        match self {
            Self::Counter { .. } => kind == MetricKind::Counter,
            Self::Gauge { .. } => kind == MetricKind::Gauge,
            Self::Histogram { .. } => kind == MetricKind::Histogram,
            Self::Summary { .. } => kind == MetricKind::Summary,
            Self::Untyped { .. } => true,
        }
    }

    /// Err(_) if there are irrecoverable error.
    /// Ok(Some(metric)) if this metric belongs to another group.
    /// Ok(None) pushed successfully.
    fn try_push(
        &mut self,
        prefix_len: usize,
        metric: Metric,
    ) -> Result<Option<Metric>, ParserError> {
        let suffix = &metric.name[prefix_len..];
        let mut key = GroupKey {
            timestamp: metric.timestamp,
            labels: metric.labels,
        };
        let value = metric.value;

        match self {
            Self::Counter(ref mut metrics)
            | Self::Gauge(ref mut metrics)
            | Self::Untyped(ref mut metrics) => {
                if !suffix.is_empty() {
                    return Ok(Some(Metric {
                        name: metric.name,
                        timestamp: key.timestamp,
                        labels: key.labels,
                        value,
                    }));
                }
                metrics.insert(key, SimpleMetric { value });
            }
            Self::Histogram(ref mut metrics) => match suffix {
                "_bucket" => {
                    let bucket = key.labels.remove("le").ok_or(ParserError::ExpectedLeTag)?;
                    let (_, bucket) = line::Metric::parse_value(&bucket)
                        .map_err(Into::into)
                        .context(ParseLabelValueSnafu)?;
                    let count = try_f64_to_u64(metric.value)?;
                    matching_group(metrics, key)
                        .buckets
                        .push(HistogramBucket { bucket, count });
                }
                "_sum" => {
                    let sum = metric.value;
                    matching_group(metrics, key).sum = sum;
                }
                "_count" => {
                    let count = try_f64_to_u64(metric.value)?;
                    matching_group(metrics, key).count = count;
                }
                _ => {
                    return Ok(Some(Metric {
                        name: metric.name,
                        timestamp: key.timestamp,
                        labels: key.labels,
                        value,
                    }))
                }
            },
            Self::Summary(ref mut metrics) => match suffix {
                "" => {
                    let quantile = key
                        .labels
                        .remove("quantile")
                        .ok_or(ParserError::ExpectedQuantileTag)?;
                    let value = metric.value;
                    let (_, quantile) = line::Metric::parse_value(&quantile)
                        .map_err(Into::into)
                        .context(ParseLabelValueSnafu)?;
                    matching_group(metrics, key)
                        .quantiles
                        .push(SummaryQuantile { quantile, value });
                }
                "_sum" => {
                    let sum = metric.value;
                    matching_group(metrics, key).sum = sum;
                }
                "_count" => {
                    let count = try_f64_to_u64(metric.value)?;
                    matching_group(metrics, key).count = count;
                }
                _ => {
                    return Ok(Some(Metric {
                        name: metric.name,
                        timestamp: key.timestamp,
                        labels: key.labels,
                        value,
                    }))
                }
            },
        }
        Ok(None)
    }
}

#[derive(Debug)]
pub struct MetricGroup {
    pub name: String,
    pub metrics: GroupKind,
}

fn try_f64_to_u64(f: f64) -> Result<u64, ParserError> {
    if 0.0 <= f && f <= u64::MAX as f64 {
        Ok(f as u64)
    } else {
        Err(ParserError::ValueOutOfRange {
            value: f,
            max: u64::MAX,
        })
    }
}

impl MetricGroup {
    fn new(name: String, kind: MetricKind) -> Self {
        let metrics = GroupKind::new(kind);
        MetricGroup { name, metrics }
    }

    // For cases where a metric group was not defined with `# TYPE ...`.
    fn new_untyped(metric: Metric) -> Self {
        let Metric {
            name,
            labels,
            value,
            timestamp,
        } = metric;
        let key = GroupKey { timestamp, labels };
        MetricGroup {
            name,
            metrics: GroupKind::new_untyped(key, value),
        }
    }

    /// `Err(_)` if there are irrecoverable error.
    /// `Ok(Some(metric))` if this metric belongs to another group.
    /// `Ok(None)` pushed successfully.
    fn try_push(&mut self, metric: Metric) -> Result<Option<Metric>, ParserError> {
        if !metric.name.starts_with(&self.name) {
            return Ok(Some(metric));
        }
        self.metrics.try_push(self.name.len(), metric)
    }
}

fn matching_group<T: Default>(values: &mut MetricMap<T>, group: GroupKey) -> &mut T {
    values.entry(group).or_default()
}

/// Parse the given text input, and group the result into higher-level
/// metric types based on the declared types in the text.
pub fn parse_text(input: &str) -> Result<Vec<MetricGroup>, ParserError> {
    let mut groups = Vec::new();

    for line in input.lines() {
        let line = Line::parse(line).with_context(|_| WithLineSnafu {
            line: line.to_owned(),
        })?;
        if let Some(line) = line {
            match line {
                Line::Header(header) => {
                    groups.push(MetricGroup::new(header.metric_name, header.kind));
                }
                Line::Metric(metric) => {
                    let metric = match groups.last_mut() {
                        Some(group) => group.try_push(metric)?,
                        None => Some(metric),
                    };
                    if let Some(metric) = metric {
                        groups.push(MetricGroup::new_untyped(metric));
                    }
                }
            }
        }
    }

    Ok(groups)
}

#[derive(Default)]
struct MetricGroupSet(IndexMap<String, GroupKind>);

impl MetricGroupSet {
    fn get_group<'a>(&'a mut self, name: &str) -> (usize, &'a String, &'a mut GroupKind) {
        let len = name.len();
        let name = if self.0.contains_key(name) {
            name
        } else if name.ends_with("_bucket") && self.0.contains_key(&name[..len - 7]) {
            &name[..len - 7]
        } else if name.ends_with("_sum") && self.0.contains_key(&name[..len - 4]) {
            &name[..len - 4]
        } else if name.ends_with("_count") && self.0.contains_key(&name[..len - 6]) {
            &name[..len - 6]
        } else {
            self.0
                .insert(name.into(), GroupKind::new(MetricKind::Untyped));
            name
        };
        self.0.get_full_mut(name).unwrap()
    }

    fn insert_metadata(&mut self, name: String, kind: MetricKind) -> Result<(), ParserError> {
        match self.0.get(&name) {
            Some(group) if !group.matches_kind(kind) => {
                Err(ParserError::MultipleMetricKinds { name })
            }
            Some(_) => Ok(()), // metadata already exists and is the right type
            None => {
                self.0.insert(name, GroupKind::new(kind));
                Ok(())
            }
        }
    }

    fn insert_sample(
        &mut self,
        name: &str,
        labels: &BTreeMap<String, String>,
        sample: proto::Sample,
    ) -> Result<(), ParserError> {
        let (_, basename, group) = self.get_group(name);
        if let Some(metric) = group.try_push(
            basename.len(),
            Metric {
                name: name.into(),
                labels: labels.clone(),
                value: sample.value,
                timestamp: Some(sample.timestamp),
            },
        )? {
            let key = GroupKey {
                timestamp: metric.timestamp,
                labels: metric.labels,
            };
            let group = GroupKind::new_untyped(key, metric.value);
            self.0.insert(metric.name, group);
        }
        Ok(())
    }

    fn finish(self) -> Vec<MetricGroup> {
        self.0
            .into_iter()
            .map(|(name, metrics)| MetricGroup { name, metrics })
            .collect()
    }
}

/// Parse the given remote_write request, grouping the metrics into
/// higher-level metric types based on the metadata.
pub fn parse_request(request: proto::WriteRequest) -> Result<Vec<MetricGroup>, ParserError> {
    let mut groups = MetricGroupSet::default();

    for metadata in request.metadata {
        let name = metadata.metric_family_name;
        let kind = proto::MetricType::try_from(metadata.r#type)
            .unwrap_or(proto::MetricType::Unknown)
            .into();
        groups.insert_metadata(name, kind)?;
    }

    for timeseries in request.timeseries {
        let mut labels: BTreeMap<String, String> = timeseries
            .labels
            .into_iter()
            .map(|label| (label.name, label.value))
            .collect();
        let name = match labels.remove(METRIC_NAME_LABEL) {
            Some(name) => name,
            None => return Err(ParserError::RequestNoNameLabel),
        };

        for sample in timeseries.samples {
            groups.insert_sample(&name, &labels, sample)?;
        }
    }

    Ok(groups.finish())
}

impl From<proto::MetricType> for MetricKind {
    fn from(kind: proto::MetricType) -> Self {
        use proto::MetricType::*;
        match kind {
            Counter => MetricKind::Counter,
            Gauge => MetricKind::Gauge,
            Histogram => MetricKind::Histogram,
            Gaugehistogram => MetricKind::Histogram,
            Summary => MetricKind::Summary,
            _ => MetricKind::Untyped,
        }
    }
}

