#![allow(missing_docs)]
use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Display, Formatter},
    fs,
    hash::Hash,
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use crate::{
    conditions,
    event::{Metric, Value},
    secrets::SecretBackends,
    serde::OneOrMany,
};

use indexmap::IndexMap;
use serde::Serialize;

use vector_config::configurable_component;
pub use vector_lib::config::{
    AcknowledgementsConfig, DataType, GlobalOptions, Input, LogNamespace,
    SourceAcknowledgementsConfig, SourceOutput, TransformOutput, WildcardMatching,
};
pub use vector_lib::configurable::component::{
    GenerateConfig, SinkDescription, TransformDescription,
};

pub mod api;
mod builder;
mod cmd;
mod compiler;
mod diff;
pub mod dot_graph;
mod enrichment_table;
pub mod format;
mod graph;
mod loading;
pub mod provider;
pub mod schema;
mod secret;
mod sink;
mod source;
mod transform;
pub mod unit_test;
mod validation;
mod vars;
pub mod watcher;

pub use builder::ConfigBuilder;
pub use cmd::{cmd, Opts};
pub use diff::ConfigDiff;
pub use enrichment_table::{EnrichmentTableConfig, EnrichmentTableOuter};
pub use format::{Format, FormatHint};
pub use loading::{
    load, load_builder_from_paths, load_from_paths, load_from_paths_with_provider_and_secrets,
    load_from_str, load_source_from_paths, merge_path_lists, process_paths, COLLECTOR,
    CONFIG_PATHS,
};
pub use provider::ProviderConfig;
pub use secret::SecretBackend;
pub use sink::{BoxedSink, SinkConfig, SinkContext, SinkHealthcheckOptions, SinkOuter};
pub use source::{BoxedSource, SourceConfig, SourceContext, SourceOuter};
pub use transform::{
    get_transform_output_ids, BoxedTransform, TransformConfig, TransformContext, TransformOuter,
};
pub use unit_test::{build_unit_tests, build_unit_tests_main, UnitTestResult};
pub use validation::warnings;
pub use vars::{interpolate, ENVIRONMENT_VARIABLE_INTERPOLATION_REGEX};
pub use vector_lib::{
    config::{
        init_log_schema, init_telemetry, log_schema, proxy::ProxyConfig, telemetry, ComponentKey,
        LogSchema, OutputId,
    },
    id::Inputs,
};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct ComponentConfig {
    pub config_paths: Vec<PathBuf>,
    pub component_key: ComponentKey,
}

impl ComponentConfig {
    pub fn new(config_paths: Vec<PathBuf>, component_key: ComponentKey) -> Self {
        let canonicalized_paths = config_paths
            .into_iter()
            .filter_map(|p| fs::canonicalize(p).ok())
            .collect();

        Self {
            config_paths: canonicalized_paths,
            component_key,
        }
    }

    pub fn contains(&self, config_paths: &[PathBuf]) -> Option<ComponentKey> {
        if config_paths.iter().any(|p| self.config_paths.contains(p)) {
            return Some(self.component_key.clone());
        }
        None
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum ConfigPath {
    File(PathBuf, FormatHint),
    Dir(PathBuf),
}

impl<'a> From<&'a ConfigPath> for &'a PathBuf {
    fn from(config_path: &'a ConfigPath) -> &'a PathBuf {
        match config_path {
            ConfigPath::File(path, _) => path,
            ConfigPath::Dir(path) => path,
        }
    }
}

impl ConfigPath {
    pub const fn as_dir(&self) -> Option<&PathBuf> {
        match self {
            Self::Dir(path) => Some(path),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Config {
    #[cfg(feature = "api")]
    pub api: api::Options,
    pub schema: schema::Options,
    pub global: GlobalOptions,
    pub healthchecks: HealthcheckOptions,
    sources: IndexMap<ComponentKey, SourceOuter>,
    sinks: IndexMap<ComponentKey, SinkOuter<OutputId>>,
    transforms: IndexMap<ComponentKey, TransformOuter<OutputId>>,
    pub enrichment_tables: IndexMap<ComponentKey, EnrichmentTableOuter<OutputId>>,
    tests: Vec<TestDefinition>,
    secret: IndexMap<ComponentKey, SecretBackends>,
    pub graceful_shutdown_duration: Option<Duration>,
}

impl Config {
    pub fn builder() -> builder::ConfigBuilder {
        Default::default()
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn sources(&self) -> impl Iterator<Item = (&ComponentKey, &SourceOuter)> {
        self.sources.iter()
    }

    pub fn source(&self, id: &ComponentKey) -> Option<&SourceOuter> {
        self.sources.get(id)
    }

    pub fn transforms(&self) -> impl Iterator<Item = (&ComponentKey, &TransformOuter<OutputId>)> {
        self.transforms.iter()
    }

    pub fn transform(&self, id: &ComponentKey) -> Option<&TransformOuter<OutputId>> {
        self.transforms.get(id)
    }

    pub fn sinks(&self) -> impl Iterator<Item = (&ComponentKey, &SinkOuter<OutputId>)> {
        self.sinks.iter()
    }

    pub fn sink(&self, id: &ComponentKey) -> Option<&SinkOuter<OutputId>> {
        self.sinks.get(id)
    }

    pub fn enrichment_tables(
        &self,
    ) -> impl Iterator<Item = (&ComponentKey, &EnrichmentTableOuter<OutputId>)> {
        self.enrichment_tables.iter()
    }

    pub fn enrichment_table(&self, id: &ComponentKey) -> Option<&EnrichmentTableOuter<OutputId>> {
        self.enrichment_tables.get(id)
    }

    pub fn inputs_for_node(&self, id: &ComponentKey) -> Option<&[OutputId]> {
        self.transforms
            .get(id)
            .map(|t| &t.inputs[..])
            .or_else(|| self.sinks.get(id).map(|s| &s.inputs[..]))
            .or_else(|| self.enrichment_tables.get(id).map(|s| &s.inputs[..]))
    }

    pub fn propagate_acknowledgements(&mut self) -> Result<(), Vec<String>> {
        let inputs: Vec<_> = self
            .sinks
            .iter()
            .filter(|(_, sink)| {
                sink.inner
                    .acknowledgements()
                    .merge_default(&self.global.acknowledgements)
                    .enabled()
            })
            .flat_map(|(name, sink)| {
                sink.inputs
                    .iter()
                    .map(|input| (name.clone(), input.clone()))
            })
            .collect();
        self.propagate_acks_rec(inputs);
        Ok(())
    }

    fn propagate_acks_rec(&mut self, sink_inputs: Vec<(ComponentKey, OutputId)>) {
        for (sink, input) in sink_inputs {
            let component = &input.component;
            if let Some(source) = self.sources.get_mut(component) {
                if source.inner.can_acknowledge() {
                    source.sink_acknowledgements = true;
                } else {
                    warn!(
                        message = "Source has acknowledgements enabled by a sink, but acknowledgements are not supported by this source. Silent data loss could occur.",
                        source = component.id(),
                        sink = sink.id(),
                    );
                }
            } else if let Some(transform) = self.transforms.get(component) {
                let inputs = transform
                    .inputs
                    .iter()
                    .map(|input| (sink.clone(), input.clone()))
                    .collect();
                self.propagate_acks_rec(inputs);
            }
        }
    }
}

/// Healthcheck options.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(default)]
pub struct HealthcheckOptions {
    /// Whether or not healthchecks are enabled for all sinks.
    ///
    /// Can be overridden on a per-sink basis.
    pub enabled: bool,

    /// Whether or not to require a sink to report as being healthy during startup.
    ///
    /// When enabled and a sink reports not being healthy, Vector will exit during start-up.
    ///
    /// Can be alternatively set, and overridden by, the `--require-healthy` command-line flag.
    pub require_healthy: bool,
}

impl HealthcheckOptions {
    pub fn set_require_healthy(&mut self, require_healthy: impl Into<Option<bool>>) {
        if let Some(require_healthy) = require_healthy.into() {
            self.require_healthy = require_healthy;
        }
    }

    const fn merge(&mut self, other: Self) {
        self.enabled &= other.enabled;
        self.require_healthy |= other.require_healthy;
    }
}

impl Default for HealthcheckOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            require_healthy: false,
        }
    }
}

/// Unique thing, like port, of which only one owner can be.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Resource {
    Port(SocketAddr, Protocol),
    SystemFdOffset(usize),
    Fd(u32),
    DiskBuffer(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Copy)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Resource {
    pub const fn tcp(addr: SocketAddr) -> Self {
        Self::Port(addr, Protocol::Tcp)
    }

    pub const fn udp(addr: SocketAddr) -> Self {
        Self::Port(addr, Protocol::Udp)
    }

    /// From given components returns all that have a resource conflict with any other component.
    pub fn conflicts<K: Eq + Hash + Clone>(
        components: impl IntoIterator<Item = (K, Vec<Resource>)>,
    ) -> HashMap<Resource, HashSet<K>> {
        let mut resource_map = HashMap::<Resource, HashSet<K>>::new();
        let mut unspecified = Vec::new();

        // Find equality based conflicts
        for (key, resources) in components {
            for resource in resources {
                if let Resource::Port(address, protocol) = &resource {
                    if address.ip().is_unspecified() {
                        unspecified.push((key.clone(), *address, *protocol));
                    }
                }

                resource_map
                    .entry(resource)
                    .or_default()
                    .insert(key.clone());
            }
        }

        // Port with unspecified address will bind to all network interfaces
        // so we have to check for all Port resources if they share the same
        // port.
        for (key, address0, protocol0) in unspecified {
            for (resource, components) in resource_map.iter_mut() {
                if let Resource::Port(address, protocol) = resource {
                    // IP addresses can either be v4 or v6.
                    // Therefore we check if the ip version matches, the port matches and if the protocol (TCP/UDP) matches
                    // when checking for equality.
                    if &address0 == address && &protocol0 == protocol {
                        components.insert(key.clone());
                    }
                }
            }
        }

        resource_map.retain(|_, components| components.len() > 1);

        resource_map
    }
}

impl Display for Protocol {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Protocol::Udp => write!(fmt, "udp"),
            Protocol::Tcp => write!(fmt, "tcp"),
        }
    }
}

impl Display for Resource {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Resource::Port(address, protocol) => write!(fmt, "{protocol} {address}"),
            Resource::SystemFdOffset(offset) => write!(fmt, "systemd {}th socket", offset + 1),
            Resource::Fd(fd) => write!(fmt, "file descriptor: {fd}"),
            Resource::DiskBuffer(name) => write!(fmt, "disk buffer {name:?}"),
        }
    }
}

/// A unit test definition.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct TestDefinition<T: 'static = OutputId> {
    /// The name of the unit test.
    pub name: String,

    /// An input event to test against.
    pub input: Option<TestInput>,

    /// A set of input events to test against.
    #[serde(default)]
    pub inputs: Vec<TestInput>,

    /// A set of expected output events after the test has run.
    #[serde(default)]
    pub outputs: Vec<TestOutput<T>>,

    /// A set of component outputs that should not have emitted any events.
    #[serde(default)]
    pub no_outputs_from: Vec<T>,
}

impl TestDefinition<String> {
    fn resolve_outputs(
        self,
        graph: &graph::Graph,
    ) -> Result<TestDefinition<OutputId>, Vec<String>> {
        let TestDefinition {
            name,
            input,
            inputs,
            outputs,
            no_outputs_from,
        } = self;
        let mut errors = Vec::new();

        let output_map = graph.input_map().expect("ambiguous outputs");

        let outputs = outputs
            .into_iter()
            .map(|old| {
                let TestOutput {
                    extract_from,
                    conditions,
                } = old;

                (extract_from.to_vec(), conditions)
            })
            .filter_map(|(extract_from, conditions)| {
                let mut outputs = Vec::new();
                for from in extract_from {
                    if let Some(output_id) = output_map.get(&from) {
                        outputs.push(output_id.clone());
                    } else {
                        errors.push(format!(
                            r#"Invalid extract_from target in test '{name}': '{from}' does not exist"#
                        ));
                    }
                }
                if outputs.is_empty() {
                    None
                } else {
                    Some(TestOutput {
                        extract_from: outputs.into(),
                        conditions,
                    })
                }
            })
            .collect();

        let no_outputs_from = no_outputs_from
            .into_iter()
            .filter_map(|o| {
                if let Some(output_id) = output_map.get(&o) {
                    Some(output_id.clone())
                } else {
                    errors.push(format!(
                        r#"Invalid no_outputs_from target in test '{name}': '{o}' does not exist"#
                    ));
                    None
                }
            })
            .collect();

        if errors.is_empty() {
            Ok(TestDefinition {
                name,
                input,
                inputs,
                outputs,
                no_outputs_from,
            })
        } else {
            Err(errors)
        }
    }
}

impl TestDefinition<OutputId> {
    fn stringify(self) -> TestDefinition<String> {
        let TestDefinition {
            name,
            input,
            inputs,
            outputs,
            no_outputs_from,
        } = self;

        let outputs = outputs
            .into_iter()
            .map(|old| TestOutput {
                extract_from: old
                    .extract_from
                    .to_vec()
                    .into_iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<_>>()
                    .into(),
                conditions: old.conditions,
            })
            .collect();

        let no_outputs_from = no_outputs_from.iter().map(ToString::to_string).collect();

        TestDefinition {
            name,
            input,
            inputs,
            outputs,
            no_outputs_from,
        }
    }
}

/// A unit test input.
///
/// An input describes not only the type of event to insert, but also which transform within the
/// configuration to insert it to.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct TestInput {
    /// The name of the transform to insert the input event to.
    pub insert_at: ComponentKey,

    /// The type of the input event.
    ///
    /// Can be either `raw`, `vrl`, `log`, or `metric.
    #[serde(default = "default_test_input_type", rename = "type")]
    pub type_str: String,

    /// The raw string value to use as the input event.
    ///
    /// Use this only when the input event should be a raw event (i.e. unprocessed/undecoded log
    /// event) and when the input type is set to `raw`.
    pub value: Option<String>,

    /// The vrl expression to generate the input event.
    ///
    /// Only relevant when `type` is `vrl`.
    pub source: Option<String>,

    /// The set of log fields to use when creating a log input event.
    ///
    /// Only relevant when `type` is `log`.
    pub log_fields: Option<IndexMap<String, Value>>,

    /// The metric to use as an input event.
    ///
    /// Only relevant when `type` is `metric`.
    pub metric: Option<Metric>,
}

fn default_test_input_type() -> String {
    "raw".to_string()
}

/// A unit test output.
///
/// An output describes what we expect a transform to emit when fed a certain event, or events, when
/// running a unit test.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct TestOutput<T: 'static = OutputId> {
    /// The transform outputs to extract events from.
    pub extract_from: OneOrMany<T>,

    /// The conditions to run against the output to validate that they were transformed as expected.
    pub conditions: Option<Vec<conditions::AnyCondition>>,
}
