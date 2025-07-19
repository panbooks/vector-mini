use std::collections::HashMap;
use std::sync::Mutex;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    path::PathBuf,
};

use snafu::{ResultExt, Snafu};
use vector_lib::codecs::MetricTagValues;
use vector_lib::compile_vrl;
use vector_lib::config::LogNamespace;
use vector_lib::configurable::configurable_component;
use vector_lib::enrichment::TableRegistry;
use vector_lib::lookup::{metadata_path, owned_value_path, PathPrefix};
use vector_lib::schema::Definition;
use vector_lib::TimeZone;
use vector_vrl_functions::set_semantic_meaning::MeaningList;
use vrl::compiler::runtime::{Runtime, Terminate};
use vrl::compiler::state::ExternalEnv;
use vrl::compiler::{CompileConfig, ExpressionError, Program, TypeState, VrlRuntime};
use vrl::diagnostic::{DiagnosticMessage, Formatter, Note};
use vrl::path;
use vrl::path::ValuePath;
use vrl::value::{Kind, Value};

use crate::config::OutputId;
use crate::{
    config::{
        log_schema, ComponentKey, DataType, Input, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{Event, TargetEvents, VrlTarget},
    internal_events::{RemapMappingAbort, RemapMappingError},
    schema,
    transforms::{SyncTransform, Transform, TransformOutputsBuf},
    Result,
};

const DROPPED: &str = "dropped";
type CacheKey = (TableRegistry, schema::Definition);
type CacheValue = (Program, String, MeaningList);

/// Configuration for the `remap` transform.
#[configurable_component(transform(
    "remap",
    "Modify your observability data as it passes through your topology using Vector Remap Language (VRL)."
))]
#[derive(Derivative)]
#[serde(deny_unknown_fields)]
#[derivative(Default, Debug)]
pub struct RemapConfig {
    /// The [Vector Remap Language][vrl] (VRL) program to execute for each event.
    ///
    /// Required if `file` is missing.
    ///
    /// [vrl]: https://vector.dev/docs/reference/vrl
    #[configurable(metadata(
        docs::examples = ". = parse_json!(.message)\n.new_field = \"new value\"\n.status = to_int!(.status)\n.duration = parse_duration!(.duration, \"s\")\n.new_name = del(.old_name)",
        docs::syntax_override = "remap_program"
    ))]
    pub source: Option<String>,

    /// File path to the [Vector Remap Language][vrl] (VRL) program to execute for each event.
    ///
    /// If a relative path is provided, its root is the current working directory.
    ///
    /// Required if `source` is missing.
    ///
    /// [vrl]: https://vector.dev/docs/reference/vrl
    #[configurable(metadata(docs::examples = "./my/program.vrl"))]
    pub file: Option<PathBuf>,

    /// File paths to the [Vector Remap Language][vrl] (VRL) programs to execute for each event.
    ///
    /// If a relative path is provided, its root is the current working directory.
    ///
    /// Required if `source` or `file` are missing.
    ///
    /// [vrl]: https://vector.dev/docs/reference/vrl
    #[configurable(metadata(docs::examples = "['./my/program.vrl', './my/program2.vrl']"))]
    pub files: Option<Vec<PathBuf>>,

    /// When set to `single`, metric tag values are exposed as single strings, the
    /// same as they were before this config option. Tags with multiple values show the last assigned value, and null values
    /// are ignored.
    ///
    /// When set to `full`, all metric tags are exposed as arrays of either string or null
    /// values.
    #[serde(default)]
    pub metric_tag_values: MetricTagValues,

    /// The name of the timezone to apply to timestamp conversions that do not contain an explicit
    /// time zone.
    ///
    /// This overrides the [global `timezone`][global_timezone] option. The time zone name may be
    /// any name in the [TZ database][tz_database], or `local` to indicate system local time.
    ///
    /// [global_timezone]: https://vector.dev/docs/reference/configuration//global-options#timezone
    /// [tz_database]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    pub timezone: Option<TimeZone>,

    /// Drops any event that encounters an error during processing.
    ///
    /// Normally, if a VRL program encounters an error when processing an event, the original,
    /// unmodified event is sent downstream. In some cases, you may not want to send the event
    /// any further, such as if certain transformation or enrichment is strictly required. Setting
    /// `drop_on_error` to `true` allows you to ensure these events do not get processed any
    /// further.
    ///
    /// Additionally, dropped events can potentially be diverted to a specially named output for
    /// further logging and analysis by setting `reroute_dropped`.
    #[serde(default = "crate::serde::default_false")]
    #[configurable(metadata(docs::human_name = "Drop Event on Error"))]
    pub drop_on_error: bool,

    /// Drops any event that is manually aborted during processing.
    ///
    /// If a VRL program is manually aborted (using [`abort`][vrl_docs_abort]) when
    /// processing an event, this option controls whether the original, unmodified event is sent
    /// downstream without any modifications or if it is dropped.
    ///
    /// Additionally, dropped events can potentially be diverted to a specially-named output for
    /// further logging and analysis by setting `reroute_dropped`.
    ///
    /// [vrl_docs_abort]: https://vector.dev/docs/reference/vrl/expressions/#abort
    #[serde(default = "crate::serde::default_true")]
    #[configurable(metadata(docs::human_name = "Drop Event on Abort"))]
    pub drop_on_abort: bool,

    /// Reroutes dropped events to a named output instead of halting processing on them.
    ///
    /// When using `drop_on_error` or `drop_on_abort`, events that are "dropped" are processed no
    /// further. In some cases, it may be desirable to keep the events around for further analysis,
    /// debugging, or retrying.
    ///
    /// In these cases, `reroute_dropped` can be set to `true` which forwards the original event
    /// to a specially-named output, `dropped`. The original event is annotated with additional
    /// fields describing why the event was dropped.
    #[serde(default = "crate::serde::default_false")]
    #[configurable(metadata(docs::human_name = "Reroute Dropped Events"))]
    pub reroute_dropped: bool,

    #[configurable(derived, metadata(docs::hidden))]
    #[serde(default)]
    pub runtime: VrlRuntime,

    #[configurable(derived, metadata(docs::hidden))]
    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    /// Cache can't be `BTreeMap` or `HashMap` because of `TableRegistry`, which doesn't allow us to inspect tables inside it.
    /// And even if we allowed the inspection, the tables can be huge, resulting in a long comparison or hash computation
    /// while using `Vec` allows us to use just a shallow comparison
    pub cache: Mutex<Vec<(CacheKey, std::result::Result<CacheValue, String>)>>,
}

impl Clone for RemapConfig {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            file: self.file.clone(),
            files: self.files.clone(),
            metric_tag_values: self.metric_tag_values,
            timezone: self.timezone,
            drop_on_error: self.drop_on_error,
            drop_on_abort: self.drop_on_abort,
            reroute_dropped: self.reroute_dropped,
            runtime: self.runtime,
            cache: Mutex::new(Default::default()),
        }
    }
}

impl RemapConfig {
    fn compile_vrl_program(
        &self,
        enrichment_tables: TableRegistry,
        merged_schema_definition: schema::Definition,
    ) -> Result<(Program, String, MeaningList)> {
        if let Some((_, res)) = self
            .cache
            .lock()
            .expect("Data poisoned")
            .iter()
            .find(|v| v.0 .0 == enrichment_tables && v.0 .1 == merged_schema_definition)
        {
            return res.clone().map_err(Into::into);
        }

        let source = match (&self.source, &self.file, &self.files) {
            (Some(source), None, None) => source.to_owned(),
            (None, Some(path), None) => Self::read_file(path)?,
            (None, None, Some(paths)) => {
                let mut combined_source = String::new();
                for path in paths {
                    let content = Self::read_file(path)?;
                    combined_source.push_str(&content);
                    combined_source.push('\n');
                }
                combined_source
            }
            _ => return Err(Box::new(BuildError::SourceAndOrFileOrFiles)),
        };

        let mut functions = vrl::stdlib::all();
        functions.append(&mut vector_lib::enrichment::vrl_functions());
        #[cfg(feature = "sources-dnstap")]
        functions.append(&mut dnstap_parser::vrl_functions());
        functions.append(&mut vector_vrl_functions::all());

        let state = TypeState {
            local: Default::default(),
            external: ExternalEnv::new_with_kind(
                merged_schema_definition.event_kind().clone(),
                merged_schema_definition.metadata_kind().clone(),
            ),
        };
        let mut config = CompileConfig::default();

        config.set_custom(enrichment_tables.clone());
        config.set_custom(MeaningList::default());

        let res = compile_vrl(&source, &functions, &state, config)
            .map_err(|diagnostics| Formatter::new(&source, diagnostics).colored().to_string())
            .map(|result| {
                (
                    result.program,
                    Formatter::new(&source, result.warnings).to_string(),
                    result.config.get_custom::<MeaningList>().unwrap().clone(),
                )
            });

        self.cache
            .lock()
            .expect("Data poisoned")
            .push(((enrichment_tables, merged_schema_definition), res.clone()));

        res.map_err(Into::into)
    }

    fn read_file(path: &PathBuf) -> Result<String> {
        let mut buffer = String::new();
        File::open(path)
            .with_context(|_| FileOpenFailedSnafu { path })?
            .read_to_string(&mut buffer)
            .with_context(|_| FileReadFailedSnafu { path })?;
        Ok(buffer)
    }
}

impl_generate_config_from_default!(RemapConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "remap")]
impl TransformConfig for RemapConfig {
    async fn build(&self, context: &TransformContext) -> Result<Transform> {
        let (transform, warnings) = match self.runtime {
            VrlRuntime::Ast => {
                let (remap, warnings) = Remap::new_ast(self.clone(), context)?;
                (Transform::synchronous(remap), warnings)
            }
        };

        // TODO: We could improve on this by adding support for non-fatal error
        // messages in the topology. This would make the topology responsible
        // for printing warnings (including potentially emitting metrics),
        // instead of individual transforms.
        if !warnings.is_empty() {
            warn!(message = "VRL compilation warning.", %warnings);
        }

        Ok(transform)
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        enrichment_tables: vector_lib::enrichment::TableRegistry,
        input_definitions: &[(OutputId, schema::Definition)],
        _: LogNamespace,
    ) -> Vec<TransformOutput> {
        let merged_definition: Definition = input_definitions
            .iter()
            .map(|(_output, definition)| definition.clone())
            .reduce(Definition::merge)
            .unwrap_or_else(Definition::any);

        // We need to compile the VRL program in order to know the schema definition output of this
        // transform. We ignore any compilation errors, as those are caught by the transform build
        // step.
        let compiled = self
            .compile_vrl_program(enrichment_tables, merged_definition)
            .map(|(program, _, meaning_list)| (program.final_type_info().state, meaning_list.0))
            .map_err(|_| ());

        let mut dropped_definitions = HashMap::new();
        let mut default_definitions = HashMap::new();

        for (output_id, input_definition) in input_definitions {
            let default_definition = compiled
                .clone()
                .map(|(state, meaning)| {
                    let mut new_type_def = Definition::new(
                        state.external.target_kind().clone(),
                        state.external.metadata_kind().clone(),
                        input_definition.log_namespaces().clone(),
                    );

                    for (id, path) in input_definition.meanings() {
                        // Attempt to copy over the meanings from the input definition.
                        // The function will fail if the meaning that now points to a field that no longer exists,
                        // this is fine since we will no longer want that meaning in the output definition.
                        let _ = new_type_def.try_with_meaning(path.clone(), id);
                    }

                    // Apply any semantic meanings set in the VRL program
                    for (id, path) in meaning {
                        // currently only event paths are supported
                        new_type_def = new_type_def.with_meaning(path, &id);
                    }
                    new_type_def
                })
                .unwrap_or_else(|_| {
                    Definition::new_with_default_metadata(
                        // The program failed to compile, so it can "never" return a value
                        Kind::never(),
                        input_definition.log_namespaces().clone(),
                    )
                });

            // When a message is dropped and re-routed, we keep the original event, but also annotate
            // it with additional metadata.
            let dropped_definition = Definition::combine_log_namespaces(
                input_definition.log_namespaces(),
                input_definition.clone().with_event_field(
                    log_schema().metadata_key().expect("valid metadata key"),
                    Kind::object(BTreeMap::from([
                        ("reason".into(), Kind::bytes()),
                        ("message".into(), Kind::bytes()),
                        ("component_id".into(), Kind::bytes()),
                        ("component_type".into(), Kind::bytes()),
                        ("component_kind".into(), Kind::bytes()),
                    ])),
                    Some("metadata"),
                ),
                input_definition
                    .clone()
                    .with_metadata_field(&owned_value_path!("reason"), Kind::bytes(), None)
                    .with_metadata_field(&owned_value_path!("message"), Kind::bytes(), None)
                    .with_metadata_field(&owned_value_path!("component_id"), Kind::bytes(), None)
                    .with_metadata_field(&owned_value_path!("component_type"), Kind::bytes(), None)
                    .with_metadata_field(&owned_value_path!("component_kind"), Kind::bytes(), None),
            );

            default_definitions.insert(
                output_id.clone(),
                VrlTarget::modify_schema_definition_for_into_events(default_definition),
            );
            dropped_definitions.insert(
                output_id.clone(),
                VrlTarget::modify_schema_definition_for_into_events(dropped_definition),
            );
        }

        let default_output = TransformOutput::new(DataType::all_bits(), default_definitions);

        if self.reroute_dropped {
            vec![
                default_output,
                TransformOutput::new(DataType::all_bits(), dropped_definitions).with_port(DROPPED),
            ]
        } else {
            vec![default_output]
        }
    }

    fn enable_concurrency(&self) -> bool {
        true
    }

    fn files_to_watch(&self) -> Vec<&PathBuf> {
        self.file
            .iter()
            .chain(self.files.iter().flatten())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Remap<Runner>
where
    Runner: VrlRunner,
{
    component_key: Option<ComponentKey>,
    program: Program,
    timezone: TimeZone,
    drop_on_error: bool,
    drop_on_abort: bool,
    reroute_dropped: bool,
    runner: Runner,
    metric_tag_values: MetricTagValues,
}

pub trait VrlRunner {
    fn run(
        &mut self,
        target: &mut VrlTarget,
        program: &Program,
        timezone: &TimeZone,
    ) -> std::result::Result<Value, Terminate>;
}

#[derive(Debug)]
pub struct AstRunner {
    pub runtime: Runtime,
}

impl Clone for AstRunner {
    fn clone(&self) -> Self {
        Self {
            runtime: Runtime::default(),
        }
    }
}

impl VrlRunner for AstRunner {
    fn run(
        &mut self,
        target: &mut VrlTarget,
        program: &Program,
        timezone: &TimeZone,
    ) -> std::result::Result<Value, Terminate> {
        let result = self.runtime.resolve(target, program, timezone);
        self.runtime.clear();
        result
    }
}

impl Remap<AstRunner> {
    pub fn new_ast(
        config: RemapConfig,
        context: &TransformContext,
    ) -> crate::Result<(Self, String)> {
        let (program, warnings, _) = config.compile_vrl_program(
            context.enrichment_tables.clone(),
            context.merged_schema_definition.clone(),
        )?;

        let runtime = Runtime::default();
        let runner = AstRunner { runtime };

        Self::new(config, context, program, runner).map(|remap| (remap, warnings))
    }
}

impl<Runner> Remap<Runner>
where
    Runner: VrlRunner,
{
    fn new(
        config: RemapConfig,
        context: &TransformContext,
        program: Program,
        runner: Runner,
    ) -> crate::Result<Self> {
        Ok(Remap {
            component_key: context.key.clone(),
            program,
            timezone: config
                .timezone
                .unwrap_or_else(|| context.globals.timezone()),
            drop_on_error: config.drop_on_error,
            drop_on_abort: config.drop_on_abort,
            reroute_dropped: config.reroute_dropped,
            runner,
            metric_tag_values: config.metric_tag_values,
        })
    }

    #[cfg(test)]
    const fn runner(&self) -> &Runner {
        &self.runner
    }

    fn dropped_data(&self, reason: &str, error: ExpressionError) -> serde_json::Value {
        let message = error
            .notes()
            .iter()
            .filter(|note| matches!(note, Note::UserErrorMessage(_)))
            .next_back()
            .map(|note| note.to_string())
            .unwrap_or_else(|| error.to_string());
        serde_json::json!({
                "reason": reason,
                "message": message,
                "component_id": self.component_key,
                "component_type": "remap",
                "component_kind": "transform",
        })
    }

    fn annotate_dropped(&self, event: &mut Event, reason: &str, error: ExpressionError) {
        match event {
            Event::Log(log) => match log.namespace() {
                LogNamespace::Legacy => {
                    if let Some(metadata_key) = log_schema().metadata_key() {
                        log.insert(
                            (PathPrefix::Event, metadata_key.concat(path!("dropped"))),
                            self.dropped_data(reason, error),
                        );
                    }
                }
                LogNamespace::Vector => {
                    log.insert(
                        metadata_path!("vector", "dropped"),
                        self.dropped_data(reason, error),
                    );
                }
            },
            Event::Metric(metric) => {
                if let Some(metadata_key) = log_schema().metadata_key() {
                    metric.replace_tag(format!("{metadata_key}.dropped.reason"), reason.into());
                    metric.replace_tag(
                        format!("{metadata_key}.dropped.component_id"),
                        self.component_key
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    );
                    metric.replace_tag(
                        format!("{metadata_key}.dropped.component_type"),
                        "remap".into(),
                    );
                    metric.replace_tag(
                        format!("{metadata_key}.dropped.component_kind"),
                        "transform".into(),
                    );
                }
            }
            Event::Trace(trace) => {
                trace.maybe_insert(log_schema().metadata_key_target_path(), || {
                    self.dropped_data(reason, error).into()
                });
            }
        }
    }

    fn run_vrl(&mut self, target: &mut VrlTarget) -> std::result::Result<Value, Terminate> {
        self.runner.run(target, &self.program, &self.timezone)
    }
}

impl<Runner> SyncTransform for Remap<Runner>
where
    Runner: VrlRunner + Clone + Send + Sync,
{
    fn transform(&mut self, event: Event, output: &mut TransformOutputsBuf) {
        // If a program can fail or abort at runtime and we know that we will still need to forward
        // the event in that case (either to the main output or `dropped`, depending on the
        // config), we need to clone the original event and keep it around, to allow us to discard
        // any mutations made to the event while the VRL program runs, before it failed or aborted.
        //
        // The `drop_on_{error, abort}` transform config allows operators to remove events from the
        // main output if they're failed or aborted, in which case we can skip the cloning, since
        // any mutations made by VRL will be ignored regardless. If they hav configured
        // `reroute_dropped`, however, we still need to do the clone to ensure that we can forward
        // the event to the `dropped` output.
        let forward_on_error = !self.drop_on_error || self.reroute_dropped;
        let forward_on_abort = !self.drop_on_abort || self.reroute_dropped;
        let original_event = if (self.program.info().fallible && forward_on_error)
            || (self.program.info().abortable && forward_on_abort)
        {
            Some(event.clone())
        } else {
            None
        };

        let log_namespace = event
            .maybe_as_log()
            .map(|log| log.namespace())
            .unwrap_or(LogNamespace::Legacy);

        let mut target = VrlTarget::new(
            event,
            self.program.info(),
            match self.metric_tag_values {
                MetricTagValues::Single => false,
                MetricTagValues::Full => true,
            },
        );
        let result = self.run_vrl(&mut target);

        match result {
            Ok(_) => match target.into_events(log_namespace) {
                TargetEvents::One(event) => push_default(event, output),
                TargetEvents::Logs(events) => events.for_each(|event| push_default(event, output)),
                TargetEvents::Traces(events) => {
                    events.for_each(|event| push_default(event, output))
                }
            },
            Err(reason) => {
                let (reason, error, drop) = match reason {
                    Terminate::Abort(error) => {
                        if !self.reroute_dropped {
                            emit!(RemapMappingAbort {
                                event_dropped: self.drop_on_abort,
                            });
                        }
                        ("abort", error, self.drop_on_abort)
                    }
                    Terminate::Error(error) => {
                        if !self.reroute_dropped {
                            emit!(RemapMappingError {
                                error: error.to_string(),
                                event_dropped: self.drop_on_error,
                            });
                        }
                        ("error", error, self.drop_on_error)
                    }
                };

                if !drop {
                    let event = original_event.expect("event will be set");

                    push_default(event, output);
                } else if self.reroute_dropped {
                    let mut event = original_event.expect("event will be set");

                    self.annotate_dropped(&mut event, reason, error);
                    push_dropped(event, output);
                }
            }
        }
    }
}

#[inline]
fn push_default(event: Event, output: &mut TransformOutputsBuf) {
    output.push(None, event)
}

#[inline]
fn push_dropped(event: Event, output: &mut TransformOutputsBuf) {
    output.push(Some(DROPPED), event);
}

#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("must provide exactly one of `source` or `file` or `files` configuration"))]
    SourceAndOrFileOrFiles,

    #[snafu(display("Could not open vrl program {:?}: {}", path, source))]
    FileOpenFailed { path: PathBuf, source: io::Error },
    #[snafu(display("Could not read vrl program {:?}: {}", path, source))]
    FileReadFailed { path: PathBuf, source: io::Error },
}

