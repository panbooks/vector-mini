use indexmap::IndexMap;
use vector_lib::config::{clone_input_definitions, LogNamespace};
use vector_lib::configurable::configurable_component;
use vector_lib::transform::SyncTransform;

use crate::{
    conditions::{AnyCondition, Condition, ConditionConfig, VrlConfig},
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::Event,
    schema,
    transforms::Transform,
};

pub(crate) const UNMATCHED_ROUTE: &str = "_unmatched";

#[derive(Clone)]
pub struct Route {
    conditions: Vec<(String, Condition)>,
    reroute_unmatched: bool,
}

impl Route {
    pub fn new(config: &RouteConfig, context: &TransformContext) -> crate::Result<Self> {
        let mut conditions = Vec::with_capacity(config.route.len());
        for (output_name, condition) in config.route.iter() {
            let condition = condition.build(&context.enrichment_tables)?;
            conditions.push((output_name.clone(), condition));
        }
        Ok(Self {
            conditions,
            reroute_unmatched: config.reroute_unmatched,
        })
    }
}

impl SyncTransform for Route {
    fn transform(&mut self, event: Event, output: &mut vector_lib::transform::TransformOutputsBuf) {
        let mut check_failed: usize = 0;
        for (output_name, condition) in &self.conditions {
            let (result, event) = condition.check(event.clone());
            if result {
                output.push(Some(output_name), event);
            } else {
                check_failed += 1;
            }
        }
        if self.reroute_unmatched && check_failed == self.conditions.len() {
            output.push(Some(UNMATCHED_ROUTE), event);
        }
    }
}

/// Configuration for the `route` transform.
#[configurable_component(transform(
    "route",
    "Split a stream of events into multiple sub-streams based on user-supplied conditions."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    /// Reroutes unmatched events to a named output instead of silently discarding them.
    ///
    /// Normally, if an event doesn't match any defined route, it is sent to the `<transform_name>._unmatched`
    /// output for further processing. In some cases, you may want to simply discard unmatched events and not
    /// process them any further.
    ///
    /// In these cases, `reroute_unmatched` can be set to `false` to disable the `<transform_name>._unmatched`
    /// output and instead silently discard any unmatched events.
    #[serde(default = "crate::serde::default_true")]
    #[configurable(metadata(docs::human_name = "Reroute Unmatched Events"))]
    reroute_unmatched: bool,

    /// A map from route identifiers to logical conditions.
    /// Each condition represents a filter which is applied to each event.
    ///
    /// The following identifiers are reserved output names and thus cannot be used as route IDs:
    /// - `_unmatched`
    /// - `_default`
    ///
    /// Each route can then be referenced as an input by other components with the name
    /// `<transform_name>.<route_id>`. If an event doesn’t match any route, and if `reroute_unmatched`
    /// is set to `true` (the default), it is sent to the `<transform_name>._unmatched` output.
    /// Otherwise, the unmatched event is instead silently discarded.
    #[configurable(metadata(docs::additional_props_description = "An individual route."))]
    #[configurable(metadata(docs::examples = "route_examples()"))]
    route: IndexMap<String, AnyCondition>,
}

fn route_examples() -> IndexMap<String, AnyCondition> {
    IndexMap::from([
        (
            "foo-exists".to_owned(),
            AnyCondition::Map(ConditionConfig::Vrl(VrlConfig {
                source: "exists(.foo)".to_owned(),
                ..Default::default()
            })),
        ),
        (
            "foo-does-not-exist".to_owned(),
            AnyCondition::Map(ConditionConfig::Vrl(VrlConfig {
                source: "!exists(.foo)".to_owned(),
                ..Default::default()
            })),
        ),
    ])
}

impl GenerateConfig for RouteConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            reroute_unmatched: true,
            route: route_examples(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "route")]
impl TransformConfig for RouteConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        let route = Route::new(self, context)?;
        Ok(Transform::synchronous(route))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn validate(&self, _: &schema::Definition) -> Result<(), Vec<String>> {
        if self.route.contains_key(UNMATCHED_ROUTE) {
            Err(vec![format!(
                "cannot have a named output with reserved name: `{UNMATCHED_ROUTE}`"
            )])
        } else {
            Ok(())
        }
    }

    fn outputs(
        &self,
        _: vector_lib::enrichment::TableRegistry,
        input_definitions: &[(OutputId, schema::Definition)],
        _: LogNamespace,
    ) -> Vec<TransformOutput> {
        let mut result: Vec<TransformOutput> = self
            .route
            .keys()
            .map(|output_name| {
                TransformOutput::new(
                    DataType::all_bits(),
                    clone_input_definitions(input_definitions),
                )
                .with_port(output_name)
            })
            .collect();
        if self.reroute_unmatched {
            result.push(
                TransformOutput::new(
                    DataType::all_bits(),
                    clone_input_definitions(input_definitions),
                )
                .with_port(UNMATCHED_ROUTE),
            );
        }
        result
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

