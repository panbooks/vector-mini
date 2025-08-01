use vector_lib::{
    config::{clone_input_definitions, LogNamespace},
    configurable::configurable_component,
};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    schema,
    transforms::Transform,
};

use super::{
    common::{default_cache_config, fill_default_fields_match, CacheConfig, FieldMatchConfig},
    transform::Dedupe,
};

/// Configuration for the `dedupe` transform.
#[configurable_component(transform("dedupe", "Deduplicate logs passing through a topology."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DedupeConfig {
    #[configurable(derived)]
    #[serde(default)]
    pub fields: Option<FieldMatchConfig>,

    #[configurable(derived)]
    #[serde(default = "default_cache_config")]
    pub cache: CacheConfig,
}

impl GenerateConfig for DedupeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            fields: None,
            cache: default_cache_config(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "dedupe")]
impl TransformConfig for DedupeConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::event_task(Dedupe::new(
            self.cache.num_events,
            fill_default_fields_match(self.fields.as_ref()),
        )))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: vector_lib::enrichment::TableRegistry,
        input_definitions: &[(OutputId, schema::Definition)],
        _: LogNamespace,
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}
