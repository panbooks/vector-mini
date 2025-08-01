use std::cmp::Ordering;

use vector_lib::event::metric::{Metric, MetricValue, Sample};

use crate::sinks::util::{
    batch::{Batch, BatchConfig, BatchError, BatchSize, PushResult},
    Merged, SinkBatchSettings,
};

mod normalize;
pub use self::normalize::*;

mod split;
pub use self::split::*;

/// The metrics buffer is a data structure for collecting a flow of data points into a batch.
///
/// Batching mostly means that we will aggregate away timestamp information, and apply metric-specific compression to
/// improve the performance of the pipeline. In particular, only the latest in a series of metrics are output, and
/// incremental metrics are summed into the output buffer. Any conversion of metrics is handled by the normalization
/// type `N: MetricNormalize`. Further, distribution metrics have their samples compressed with
/// `compress_distribution` below.
///
/// Note: This has been deprecated, please do not use when creating new Sinks.
pub struct MetricsBuffer {
    metrics: Option<MetricSet>,
    max_events: usize,
}

impl MetricsBuffer {
    /// Creates a new `MetricsBuffer` with the given batch settings.
    pub const fn new(settings: BatchSize<Self>) -> Self {
        Self::with_capacity(settings.events)
    }

    const fn with_capacity(max_events: usize) -> Self {
        Self {
            metrics: None,
            max_events,
        }
    }
}

impl Batch for MetricsBuffer {
    type Input = Metric;
    type Output = Vec<Metric>;

    fn get_settings_defaults<D: SinkBatchSettings + Clone>(
        config: BatchConfig<D, Merged>,
    ) -> Result<BatchConfig<D, Merged>, BatchError> {
        config.disallow_max_bytes()
    }

    fn push(&mut self, item: Self::Input) -> PushResult<Self::Input> {
        if self.num_items() >= self.max_events {
            PushResult::Overflow(item)
        } else {
            let max_events = self.max_events;
            self.metrics
                .get_or_insert_with(|| MetricSet::with_capacity(max_events))
                .insert_update(item);
            PushResult::Ok(self.num_items() >= self.max_events)
        }
    }

    fn is_empty(&self) -> bool {
        self.num_items() == 0
    }

    fn fresh(&self) -> Self {
        Self::with_capacity(self.max_events)
    }

    fn finish(self) -> Self::Output {
        // Collect all of our metrics together, finalize them, and hand them back.
        let mut finalized = self
            .metrics
            .map(MetricSet::into_metrics)
            .unwrap_or_default();
        finalized.iter_mut().for_each(finalize_metric);
        finalized
    }

    fn num_items(&self) -> usize {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.len())
            .unwrap_or(0)
    }
}

fn finalize_metric(metric: &mut Metric) {
    if let MetricValue::Distribution { samples, .. } = metric.data_mut().value_mut() {
        let compressed_samples = compress_distribution(samples);
        *samples = compressed_samples;
    }
}

pub fn compress_distribution(samples: &mut Vec<Sample>) -> Vec<Sample> {
    if samples.is_empty() {
        return Vec::new();
    }

    samples.sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal));

    let mut acc = Sample {
        value: samples[0].value,
        rate: 0,
    };
    let mut result = Vec::new();

    for sample in samples {
        if acc.value == sample.value {
            acc.rate += sample.rate;
        } else {
            result.push(acc);
            acc = *sample;
        }
    }
    result.push(acc);

    result
}
