use futures::StreamExt;
#[cfg(target_os = "linux")]
use heim::net::os::linux::IoCountersExt;
#[cfg(windows)]
use heim::net::os::windows::IoCountersExt;
use heim::units::information::byte;
use vector_lib::configurable::configurable_component;
use vector_lib::metric_tags;

use crate::internal_events::HostMetricsScrapeDetailError;

use super::{default_all_devices, example_devices, filter_result, FilterList, HostMetrics};

/// Options for the network metrics collector.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct NetworkConfig {
    /// Lists of device name patterns to include or exclude in gathering
    /// network utilization metrics.
    #[serde(default = "default_all_devices")]
    #[configurable(metadata(docs::examples = "example_devices()"))]
    devices: FilterList,
}

impl HostMetrics {
    pub async fn network_metrics(&self, output: &mut super::MetricsBuffer) {
        output.name = "network";
        match heim::net::io_counters().await {
            Ok(counters) => {
                for counter in counters
                    .filter_map(|result| {
                        filter_result(result, "Failed to load/parse network data.")
                    })
                    // The following pair should be possible to do in one
                    // .filter_map, but it results in a strange "one type is
                    // more general than the other" error.
                    .map(|counter| {
                        self.config
                            .network
                            .devices
                            .contains_str(Some(counter.interface()))
                            .then_some(counter)
                    })
                    .filter_map(|counter| async { counter })
                    .collect::<Vec<_>>()
                    .await
                {
                    let interface = counter.interface();
                    let tags = metric_tags!("device" => interface);
                    output.counter(
                        "network_receive_bytes_total",
                        counter.bytes_recv().get::<byte>() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "network_receive_errs_total",
                        counter.errors_recv() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "network_receive_packets_total",
                        counter.packets_recv() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "network_transmit_bytes_total",
                        counter.bytes_sent().get::<byte>() as f64,
                        tags.clone(),
                    );
                    #[cfg(any(target_os = "linux", windows))]
                    output.counter(
                        "network_transmit_packets_drop_total",
                        counter.drop_sent() as f64,
                        tags.clone(),
                    );
                    #[cfg(any(target_os = "linux", windows))]
                    output.counter(
                        "network_transmit_packets_total",
                        counter.packets_sent() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "network_transmit_errs_total",
                        counter.errors_sent() as f64,
                        tags,
                    );
                }
            }
            Err(error) => {
                emit!(HostMetricsScrapeDetailError {
                    message: "Failed to load network I/O counters.",
                    error,
                });
            }
        }
    }
}
