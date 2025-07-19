use chrono::Utc;
use futures::{stream, StreamExt};
use vector_lib::codecs::BytesDeserializerConfig;
use vector_lib::config::log_schema;
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::lookup_v2::OptionalValuePath;
use vector_lib::lookup::{owned_value_path, path, OwnedValuePath};
use vector_lib::{
    config::{LegacyKey, LogNamespace},
    schema::Definition,
};
use vrl::value::Kind;

use crate::{
    config::{DataType, SourceConfig, SourceContext, SourceOutput},
    event::{EstimatedJsonEncodedSizeOf, Event},
    internal_events::{InternalLogsBytesReceived, InternalLogsEventsReceived, StreamClosedError},
    shutdown::ShutdownSignal,
    trace::TraceSubscription,
    SourceSender,
};

/// Configuration for the `internal_logs` source.
#[configurable_component(source(
    "internal_logs",
    "Expose internal log messages emitted by the running Vector instance."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct InternalLogsConfig {
    /// Overrides the name of the log field used to add the current hostname to each event.
    ///
    /// By default, the [global `log_schema.host_key` option][global_host_key] is used.
    ///
    /// Set to `""` to suppress this key.
    ///
    /// [global_host_key]: https://vector.dev/docs/reference/configuration/global-options/#log_schema.host_key
    host_key: Option<OptionalValuePath>,

    /// Overrides the name of the log field used to add the current process ID to each event.
    ///
    /// By default, `"pid"` is used.
    ///
    /// Set to `""` to suppress this key.
    #[serde(default = "default_pid_key")]
    pid_key: OptionalValuePath,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

fn default_pid_key() -> OptionalValuePath {
    OptionalValuePath::from(owned_value_path!("pid"))
}

impl_generate_config_from_default!(InternalLogsConfig);

impl Default for InternalLogsConfig {
    fn default() -> InternalLogsConfig {
        InternalLogsConfig {
            host_key: None,
            pid_key: default_pid_key(),
            log_namespace: None,
        }
    }
}

impl InternalLogsConfig {
    /// Generates the `schema::Definition` for this component.
    fn schema_definition(&self, log_namespace: LogNamespace) -> Definition {
        let host_key = self
            .host_key
            .clone()
            .unwrap_or(log_schema().host_key().cloned().into())
            .path
            .map(LegacyKey::Overwrite);
        let pid_key = self.pid_key.clone().path.map(LegacyKey::Overwrite);

        // There is a global and per-source `log_namespace` config.
        // The source config overrides the global setting and is merged here.
        BytesDeserializerConfig
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                InternalLogsConfig::NAME,
                host_key,
                &owned_value_path!("host"),
                Kind::bytes().or_undefined(),
                Some("host"),
            )
            .with_source_metadata(
                InternalLogsConfig::NAME,
                pid_key,
                &owned_value_path!("pid"),
                Kind::integer(),
                None,
            )
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "internal_logs")]
impl SourceConfig for InternalLogsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let host_key = self
            .host_key
            .clone()
            .unwrap_or(log_schema().host_key().cloned().into())
            .path;
        let pid_key = self.pid_key.clone().path;

        let subscription = TraceSubscription::subscribe();

        let log_namespace = cx.log_namespace(self.log_namespace);

        Ok(Box::pin(run(
            host_key,
            pid_key,
            subscription,
            cx.out,
            cx.shutdown,
            log_namespace,
        )))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let schema_definition =
            self.schema_definition(global_log_namespace.merge(self.log_namespace));

        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

async fn run(
    host_key: Option<OwnedValuePath>,
    pid_key: Option<OwnedValuePath>,
    mut subscription: TraceSubscription,
    mut out: SourceSender,
    shutdown: ShutdownSignal,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    let hostname = crate::get_hostname();
    let pid = std::process::id();

    // Chain any log events that were captured during early buffering to the front,
    // and then continue with the normal stream of internal log events.
    let buffered_events = subscription.buffered_events().await;
    let mut rx = stream::iter(buffered_events.into_iter().flatten())
        .chain(subscription.into_stream())
        .take_until(shutdown);

    // Note: This loop, or anything called within it, MUST NOT generate
    // any logs that don't break the loop, as that could cause an
    // infinite loop since it receives all such logs.
    while let Some(mut log) = rx.next().await {
        // TODO: Should this actually be in memory size?
        let byte_size = log.estimated_json_encoded_size_of().get();
        let json_byte_size = log.estimated_json_encoded_size_of();
        // This event doesn't emit any log
        emit!(InternalLogsBytesReceived { byte_size });
        emit!(InternalLogsEventsReceived {
            count: 1,
            byte_size: json_byte_size,
        });

        if let Ok(hostname) = &hostname {
            let legacy_host_key = host_key.as_ref().map(LegacyKey::Overwrite);
            log_namespace.insert_source_metadata(
                InternalLogsConfig::NAME,
                &mut log,
                legacy_host_key,
                path!("host"),
                hostname.to_owned(),
            );
        }

        let legacy_pid_key = pid_key.as_ref().map(LegacyKey::Overwrite);
        log_namespace.insert_source_metadata(
            InternalLogsConfig::NAME,
            &mut log,
            legacy_pid_key,
            path!("pid"),
            pid,
        );

        log_namespace.insert_standard_vector_source_metadata(
            &mut log,
            InternalLogsConfig::NAME,
            Utc::now(),
        );

        if (out.send_event(Event::from(log)).await).is_err() {
            // this wont trigger any infinite loop considering it stops the component
            emit!(StreamClosedError { count: 1 });
            return Err(());
        }
    }

    Ok(())
}

