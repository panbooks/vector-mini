mod ddsketch;
mod label_filter;
mod metric_matcher;
mod recency;
mod recorder;
mod storage;

use std::{sync::OnceLock, time::Duration};

use chrono::Utc;
use metric_matcher::MetricKeyMatcher;
use metrics::Key;
use metrics_tracing_context::TracingContextLayer;
use metrics_util::layers::Layer;
use snafu::Snafu;

pub use self::ddsketch::{AgentDDSketch, BinMap, Config};
use self::{label_filter::VectorLabelFilter, recorder::Registry, recorder::VectorRecorder};
use crate::{
    config::metrics_expiration::PerMetricSetExpiration,
    event::{Metric, MetricValue},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Snafu)]
pub enum Error {
    #[snafu(display("Recorder already initialized."))]
    AlreadyInitialized,
    #[snafu(display("Metrics system was not initialized."))]
    NotInitialized,
    #[snafu(display("Timeout value of {} must be positive.", timeout))]
    TimeoutMustBePositive { timeout: f64 },
    #[snafu(display("Invalid regex pattern: {}.", pattern))]
    InvalidRegexPattern { pattern: String },
}

static CONTROLLER: OnceLock<Controller> = OnceLock::new();

// Cardinality counter parameters, expose the internal metrics registry
// cardinality. Useful for the end users to help understand the characteristics
// of their environment and how vectors acts in it.
const CARDINALITY_KEY_NAME: &str = "internal_metrics_cardinality";
static CARDINALITY_KEY: Key = Key::from_static_name(CARDINALITY_KEY_NAME);

// Older deprecated counter key name
const CARDINALITY_COUNTER_KEY_NAME: &str = "internal_metrics_cardinality_total";
static CARDINALITY_COUNTER_KEY: Key = Key::from_static_name(CARDINALITY_COUNTER_KEY_NAME);

/// Controller allows capturing metric snapshots.
pub struct Controller {
    recorder: VectorRecorder,
}

fn metrics_enabled() -> bool {
    !matches!(std::env::var("DISABLE_INTERNAL_METRICS_CORE"), Ok(x) if x == "true")
}

fn tracing_context_layer_enabled() -> bool {
    !matches!(std::env::var("DISABLE_INTERNAL_METRICS_TRACING_INTEGRATION"), Ok(x) if x == "true")
}

fn init(recorder: VectorRecorder) -> Result<()> {
    // An escape hatch to allow disabling internal metrics core. May be used for
    // performance reasons. This is a hidden and undocumented functionality.
    if !metrics_enabled() {
        metrics::set_global_recorder(metrics::NoopRecorder)
            .map_err(|_| Error::AlreadyInitialized)?;
        info!(message = "Internal metrics core is disabled.");
        return Ok(());
    }

    ////
    //// Initialize the recorder.
    ////

    // The recorder is the interface between metrics-rs and our registry. In our
    // case it doesn't _do_ much other than shepherd into the registry and
    // update the cardinality counter, see above, as needed.
    if tracing_context_layer_enabled() {
        // Apply a layer to capture tracing span fields as labels.
        metrics::set_global_recorder(
            TracingContextLayer::new(VectorLabelFilter).layer(recorder.clone()),
        )
        .map_err(|_| Error::AlreadyInitialized)?;
    } else {
        metrics::set_global_recorder(recorder.clone()).map_err(|_| Error::AlreadyInitialized)?;
    }

    ////
    //// Prepare the controller
    ////

    // The `Controller` is a safe spot in memory for us to stash a clone of the registry -- where
    // metrics are actually kept -- so that our sub-systems interested in these metrics can grab
    // copies. See `capture_metrics` and its callers for an example. Note that this is done last to
    // allow `init_test` below to use the initialization state of `CONTROLLER` to wait for the above
    // steps to complete in another thread.
    let controller = Controller { recorder };
    CONTROLLER
        .set(controller)
        .map_err(|_| Error::AlreadyInitialized)?;

    Ok(())
}

/// Initialize the default metrics sub-system
///
/// # Errors
///
/// This function will error if it is called multiple times.
pub fn init_global() -> Result<()> {
    init(VectorRecorder::new_global())
}

/// Initialize the thread-local metrics sub-system. This function will loop until a recorder is
/// actually set.
pub fn init_test() {
    if init(VectorRecorder::new_test()).is_err() {
        // The only error case returned by `init` is `AlreadyInitialized`. A race condition is
        // possible here: if metrics are being initialized by two (or more) test threads
        // simultaneously, the ones that fail to set return immediately, possibly allowing
        // subsequent code to execute before the static recorder value is actually set within the
        // `metrics` crate. To prevent subsequent code from running with an unset recorder, loop
        // here until a recorder is available.
        while CONTROLLER.get().is_none() {}
    }
}

impl Controller {
    /// Clear all metrics from the registry.
    pub fn reset(&self) {
        self.recorder.with_registry(Registry::clear);
    }

    /// Get a handle to the globally registered controller, if it's initialized.
    ///
    /// # Errors
    ///
    /// This function will fail if the metrics subsystem has not been correctly
    /// initialized.
    pub fn get() -> Result<&'static Self> {
        CONTROLLER.get().ok_or(Error::NotInitialized)
    }

    /// Set or clear the expiry time after which idle metrics are dropped from the set of captured
    /// metrics. Invalid timeouts (zero or negative values) are silently remapped to no expiry.
    ///
    /// # Errors
    ///
    /// The contained timeout value must be positive.
    pub fn set_expiry(
        &self,
        global_timeout: Option<f64>,
        expire_metrics_per_metric_set: Vec<PerMetricSetExpiration>,
    ) -> Result<()> {
        if let Some(timeout) = global_timeout {
            if timeout <= 0.0 {
                return Err(Error::TimeoutMustBePositive { timeout });
            }
        }
        let per_metric_expiration = expire_metrics_per_metric_set
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<(MetricKeyMatcher, Duration)>>>()?;

        self.recorder.with_registry(|registry| {
            registry.set_expiry(
                global_timeout.map(Duration::from_secs_f64),
                per_metric_expiration,
            );
        });
        Ok(())
    }

    /// Take a snapshot of all gathered metrics and expose them as metric
    /// [`Event`](crate::event::Event)s.
    pub fn capture_metrics(&self) -> Vec<Metric> {
        let timestamp = Utc::now();

        let mut metrics = self.recorder.with_registry(Registry::visit_metrics);

        #[allow(clippy::cast_precision_loss)]
        let value = (metrics.len() + 2) as f64;
        metrics.push(Metric::from_metric_kv(
            &CARDINALITY_KEY,
            MetricValue::Gauge { value },
            timestamp,
        ));
        metrics.push(Metric::from_metric_kv(
            &CARDINALITY_COUNTER_KEY,
            MetricValue::Counter { value },
            timestamp,
        ));

        metrics
    }
}

#[macro_export]
/// This macro is used to emit metrics as a `counter` while simultaneously
/// converting from absolute values to incremental values.
///
/// Values that do not arrive in strictly monotonically increasing order are
/// ignored and will not be emitted.
macro_rules! update_counter {
    ($label:literal, $value:expr) => {{
        use ::std::sync::atomic::{AtomicU64, Ordering};

        static PREVIOUS_VALUE: AtomicU64 = AtomicU64::new(0);

        let new_value = $value;
        let mut previous_value = PREVIOUS_VALUE.load(Ordering::Relaxed);

        loop {
            // Either a new greater value has been emitted before this thread updated the counter
            // or values were provided that are not in strictly monotonically increasing order.
            // Ignore.
            if new_value <= previous_value {
                break;
            }

            match PREVIOUS_VALUE.compare_exchange_weak(
                previous_value,
                new_value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                // Another thread has written a new value before us. Re-enter loop.
                Err(value) => previous_value = value,
                // Calculate delta to last emitted value and emit it.
                Ok(_) => {
                    let delta = new_value - previous_value;
                    // Albeit very unlikely, note that this sequence of deltas might be emitted in
                    // a different order than they were calculated.
                    counter!($label).increment(delta);
                    break;
                }
            }
        }
    }};
}

