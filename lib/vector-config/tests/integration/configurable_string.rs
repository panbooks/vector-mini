use std::collections::{BTreeMap, HashMap};
use std::fmt;

use indexmap::IndexMap;
use vector_config::{configurable_component, schema::generate_root_schema, ConfigurableString};

/// A type that pretends to be `ConfigurableString` but has a non-string-like schema.
#[configurable_component]
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FakeString(u64);

impl ConfigurableString for FakeString {}

impl fmt::Display for FakeString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Implement the fmt method to define the display format
        write!(f, "{}", self.0)
    }
}



