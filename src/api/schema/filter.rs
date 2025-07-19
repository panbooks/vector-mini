use std::collections::BTreeSet;

use async_graphql::{InputObject, InputType};

use super::components::{source, ComponentKind};

/// Takes an `&Option<bool>` and returns early if false
#[macro_export]
macro_rules! filter_check {
    ($($match:expr_2021),+) => {
        $(
            if matches!($match, Some(t) if !t) {
                return false;
            }
        )+
    }
}

#[derive(Default, InputObject)]
/// Filter for String values
pub struct StringFilter {
    pub equals: Option<String>,
    pub not_equals: Option<String>,
    pub contains: Option<String>,
    pub not_contains: Option<String>,
    pub starts_with: Option<String>,
    pub ends_with: Option<String>,
}

impl StringFilter {
    pub fn filter_value(&self, value: &str) -> bool {
        filter_check!(
            // Equals
            self.equals.as_ref().map(|s| value.eq(s)),
            // Not equals
            self.not_equals.as_ref().map(|s| !value.eq(s)),
            // Contains
            self.contains.as_ref().map(|s| value.contains(s)),
            // Does not contain
            self.not_contains.as_ref().map(|s| !value.contains(s)),
            // Starts with
            self.starts_with.as_ref().map(|s| value.starts_with(s)),
            // Ends with
            self.ends_with.as_ref().map(|s| value.ends_with(s))
        );
        true
    }
}

#[derive(InputObject)]
#[graphql(concrete(name = "SourceOutputTypeFilter", params(source::SourceOutputType)))]
// Filter for GraphQL lists
pub struct ListFilter<T: InputType + PartialEq + Eq + Ord> {
    pub equals: Option<Vec<T>>,
    pub not_equals: Option<Vec<T>>,
    pub contains: Option<T>,
    pub not_contains: Option<T>,
}

impl<T: InputType + PartialEq + Eq + Ord> ListFilter<T> {
    pub fn filter_value(&self, value: Vec<T>) -> bool {
        let val = BTreeSet::from_iter(value.iter());
        filter_check!(
            // Equals
            self.equals
                .as_ref()
                .map(|s| BTreeSet::from_iter(s.iter()).eq(&val)),
            // Not Equals
            self.not_equals
                .as_ref()
                .map(|s| !BTreeSet::from_iter(s.iter()).eq(&val)),
            // Contains
            self.contains.as_ref().map(|s| val.contains(s)),
            // Not Contains
            self.not_contains.as_ref().map(|s| !val.contains(s))
        );
        true
    }
}

#[derive(InputObject)]
#[graphql(concrete(name = "ComponentKindFilter", params(ComponentKind)))]
pub struct EqualityFilter<T: InputType + PartialEq + Eq> {
    pub equals: Option<T>,
    pub not_equals: Option<T>,
}

impl<T: InputType + PartialEq + Eq> EqualityFilter<T> {
    pub fn filter_value(&self, value: T) -> bool {
        filter_check!(
            // Equals
            self.equals.as_ref().map(|s| value.eq(s)),
            // Not equals
            self.not_equals.as_ref().map(|s| !value.eq(s))
        );
        true
    }
}

/// CustomFilter trait to determine whether to include/exclude fields based on matches.
pub trait CustomFilter<T> {
    fn matches(&self, item: &T) -> bool;
    fn or(&self) -> Option<&Vec<Self>>
    where
        Self: Sized;
}

/// Returns true if a provided `Item` passes all 'AND' or 'OR' filter rules, recursively.
fn filter_item<Item, Filter>(item: &Item, f: &Filter) -> bool
where
    Filter: CustomFilter<Item>,
{
    f.matches(item)
        || f.or()
            .map_or_else(|| false, |f| f.iter().any(|f| filter_item(item, f)))
}

/// Filters items based on an implementation of `CustomFilter<T>`.
pub fn filter_items<Item, Iter, Filter>(items: Iter, f: &Filter) -> Vec<Item>
where
    Iter: Iterator<Item = Item>,
    Filter: CustomFilter<Item>,
{
    items.filter(|c| filter_item(c, f)).collect()
}

