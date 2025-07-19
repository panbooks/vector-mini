use std::time::Duration;

use futures::StreamExt;
use serde_with::serde_as;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vector_lib::configurable::configurable_component;
use vector_lib::internal_event::{
    ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol,
};
use vector_lib::lookup::lookup_v2::OptionalValuePath;
use vector_lib::{config::LogNamespace, ByteSizeOf, EstimatedJsonEncodedSizeOf};

use crate::{
    config::{log_schema, SourceConfig, SourceContext, SourceOutput},
    internal_events::{EventsReceived, StreamClosedError},
    metrics::Controller,
    shutdown::ShutdownSignal,
    SourceSender,
};

/// Configuration for the `internal_metrics` source.
#[serde_as]
#[configurable_component(source(
    "internal_metrics",
    "Expose internal metrics emitted by the running Vector instance."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct InternalMetricsConfig {
    /// The interval between metric gathering, in seconds.
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[serde(default = "default_scrape_interval")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    pub scrape_interval_secs: Duration,

    #[configurable(derived)]
    pub tags: TagsConfig,

    /// Overrides the default namespace for the metrics emitted by the source.
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

impl Default for InternalMetricsConfig {
    fn default() -> Self {
        Self {
            scrape_interval_secs: default_scrape_interval(),
            tags: TagsConfig::default(),
            namespace: default_namespace(),
        }
    }
}

/// Tag configuration for the `internal_metrics` source.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields, default)]
pub struct TagsConfig {
    /// Overrides the name of the tag used to add the peer host to each metric.
    ///
    /// The value is the peer host's address, including the port. For example, `1.2.3.4:9000`.
    ///
    /// By default, the [global `log_schema.host_key` option][global_host_key] is used.
    ///
    /// Set to `""` to suppress this key.
    ///
    /// [global_host_key]: https://vector.dev/docs/reference/configuration/global-options/#log_schema.host_key
    pub host_key: Option<OptionalValuePath>,

    /// Sets the name of the tag to use to add the current process ID to each metric.
    ///
    ///
    /// By default, this is not set and the tag is not automatically added.
    #[configurable(metadata(docs::examples = "pid"))]
    pub pid_key: Option<String>,
}

fn default_scrape_interval() -> Duration {
    Duration::from_secs_f64(1.0)
}

fn default_namespace() -> String {
    "vector".to_owned()
}

impl_generate_config_from_default!(InternalMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "internal_metrics")]
impl SourceConfig for InternalMetricsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        if self.scrape_interval_secs.is_zero() {
            warn!(
                "Interval set to 0 secs, this could result in high CPU utilization. It is suggested to use interval >= 1 secs.",
            );
        }
        let interval = self.scrape_interval_secs;

        // namespace for created metrics is already "vector" by default.
        let namespace = self.namespace.clone();

        let host_key = self
            .tags
            .host_key
            .clone()
            .unwrap_or(log_schema().host_key().cloned().into());

        let pid_key = self
            .tags
            .pid_key
            .as_deref()
            .and_then(|tag| (!tag.is_empty()).then(|| tag.to_owned()));

        Ok(Box::pin(
            InternalMetrics {
                namespace,
                host_key,
                pid_key,
                controller: Controller::get()?,
                interval,
                out: cx.out,
                shutdown: cx.shutdown,
            }
            .run(),
        ))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

struct InternalMetrics<'a> {
    namespace: String,
    host_key: OptionalValuePath,
    pid_key: Option<String>,
    controller: &'a Controller,
    interval: time::Duration,
    out: SourceSender,
    shutdown: ShutdownSignal,
}

impl InternalMetrics<'_> {
    async fn run(mut self) -> Result<(), ()> {
        let events_received = register!(EventsReceived);
        let bytes_received = register!(BytesReceived::from(Protocol::INTERNAL));
        let mut interval =
            IntervalStream::new(time::interval(self.interval)).take_until(self.shutdown);
        while interval.next().await.is_some() {
            let hostname = crate::get_hostname();
            let pid = std::process::id().to_string();

            let metrics = self.controller.capture_metrics();
            let count = metrics.len();
            let byte_size = metrics.size_of();
            let json_size = metrics.estimated_json_encoded_size_of();

            bytes_received.emit(ByteSize(byte_size));
            events_received.emit(CountByteSize(count, json_size));

            let batch = metrics.into_iter().map(|mut metric| {
                // A metric starts out with a default "vector" namespace, but will be overridden
                // if an explicit namespace is provided to this source.
                if self.namespace != "vector" {
                    metric = metric.with_namespace(Some(self.namespace.clone()));
                }

                if let Some(host_key) = &self.host_key.path {
                    if let Ok(hostname) = &hostname {
                        metric.replace_tag(host_key.to_string(), hostname.to_owned());
                    }
                }
                if let Some(pid_key) = &self.pid_key {
                    metric.replace_tag(pid_key.to_owned(), pid.clone());
                }
                metric
            });

            if (self.out.send_batch(batch).await).is_err() {
                emit!(StreamClosedError { count });
                return Err(());
            }
        }

        Ok(())
    }
}

