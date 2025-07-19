use std::cmp;

use async_graphql::{Enum, InputObject, Object};

use super::{sink, state, transform, Component};
use crate::{
    api::schema::{
        filter,
        metrics::{self, outputs_by_component_key, IntoSourceMetrics, Output},
        sort,
    },
    config::{ComponentKey, DataType, OutputId},
    filter_check,
};

#[derive(Debug, Enum, Eq, PartialEq, Copy, Clone, Ord, PartialOrd)]
pub enum SourceOutputType {
    Log,
    Metric,
    Trace,
}

#[derive(Debug, Clone)]
pub struct Data {
    pub component_key: ComponentKey,
    pub component_type: String,
    pub output_type: DataType,
    pub outputs: Vec<String>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum SourcesSortFieldName {
    ComponentKey,
    ComponentType,
    OutputType,
}

#[derive(Debug, Clone)]
pub struct Source(pub Data);

impl Source {
    #[allow(clippy::missing_const_for_fn)] // const cannot run destructor
    pub fn get_component_key(&self) -> &ComponentKey {
        &self.0.component_key
    }
    pub fn get_component_type(&self) -> &str {
        self.0.component_type.as_str()
    }
    pub fn get_output_types(&self) -> Vec<SourceOutputType> {
        [
            SourceOutputType::Log,
            SourceOutputType::Metric,
            SourceOutputType::Trace,
        ]
        .iter()
        .copied()
        .filter(|s| self.0.output_type.contains(s.into()))
        .collect()
    }

    pub fn get_outputs(&self) -> &[String] {
        self.0.outputs.as_ref()
    }
}

impl From<&SourceOutputType> for DataType {
    fn from(s: &SourceOutputType) -> Self {
        match s {
            SourceOutputType::Log => DataType::Log,
            SourceOutputType::Metric => DataType::Metric,
            SourceOutputType::Trace => DataType::Trace,
        }
    }
}

impl sort::SortableByField<SourcesSortFieldName> for Source {
    fn sort(&self, rhs: &Self, field: &SourcesSortFieldName) -> cmp::Ordering {
        match field {
            SourcesSortFieldName::ComponentKey => {
                Ord::cmp(self.get_component_key(), rhs.get_component_key())
            }
            SourcesSortFieldName::ComponentType => {
                Ord::cmp(self.get_component_type(), rhs.get_component_type())
            }
            SourcesSortFieldName::OutputType => {
                Ord::cmp(&u8::from(self.0.output_type), &u8::from(rhs.0.output_type))
            }
        }
    }
}

#[Object]
impl Source {
    /// Source component_id
    pub async fn component_id(&self) -> &str {
        self.0.component_key.id()
    }

    /// Source type
    pub async fn component_type(&self) -> &str {
        self.get_component_type()
    }

    /// Source output type
    pub async fn output_types(&self) -> Vec<SourceOutputType> {
        self.get_output_types()
    }

    /// Source output streams
    pub async fn outputs(&self) -> Vec<Output> {
        outputs_by_component_key(self.get_component_key(), self.get_outputs())
    }

    /// Transform outputs
    pub async fn transforms(&self) -> Vec<transform::Transform> {
        state::filter_components(|(_component_key, components)| match components {
            Component::Transform(t)
                if t.0.inputs.contains(&OutputId::from(&self.0.component_key)) =>
            {
                Some(t.clone())
            }
            _ => None,
        })
    }

    /// Sink outputs
    pub async fn sinks(&self) -> Vec<sink::Sink> {
        state::filter_components(|(_component_key, components)| match components {
            Component::Sink(s) if s.0.inputs.contains(&OutputId::from(&self.0.component_key)) => {
                Some(s.clone())
            }
            _ => None,
        })
    }

    /// Source metrics
    pub async fn metrics(&self) -> metrics::SourceMetrics {
        metrics::by_component_key(&self.0.component_key)
            .into_source_metrics(self.get_component_type())
    }
}

#[derive(Default, InputObject)]
pub(super) struct SourcesFilter {
    component_id: Option<Vec<filter::StringFilter>>,
    component_type: Option<Vec<filter::StringFilter>>,
    output_type: Option<Vec<filter::ListFilter<SourceOutputType>>>,
    or: Option<Vec<Self>>,
}

impl filter::CustomFilter<Source> for SourcesFilter {
    fn matches(&self, source: &Source) -> bool {
        filter_check!(
            self.component_id.as_ref().map(|f| f
                .iter()
                .all(|f| f.filter_value(&source.get_component_key().to_string()))),
            self.component_type.as_ref().map(|f| f
                .iter()
                .all(|f| f.filter_value(source.get_component_type()))),
            self.output_type
                .as_ref()
                .map(|f| f.iter().all(|f| f.filter_value(source.get_output_types())))
        );
        true
    }

    fn or(&self) -> Option<&Vec<Self>> {
        self.or.as_ref()
    }
}

