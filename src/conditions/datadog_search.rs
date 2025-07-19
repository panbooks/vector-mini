use std::{borrow::Cow, str::FromStr};
use vrl::path::PathParseError;

use bytes::Bytes;
use vector_lib::configurable::configurable_component;
use vector_lib::event::{Event, LogEvent, Value};
use vrl::datadog_filter::regex::{wildcard_regex, word_regex};
use vrl::datadog_filter::{build_matcher, Filter, Matcher, Resolver, Run};
use vrl::datadog_search_syntax::{Comparison, ComparisonValue, Field, QueryNode};

use super::{Condition, Conditional, ConditionalConfig};

/// A condition that uses the [Datadog Search](https://docs.datadoghq.com/logs/explorer/search_syntax/) query syntax against an event.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
pub struct DatadogSearchConfig {
    /// The query string.
    source: QueryNode,
}

impl Default for DatadogSearchConfig {
    fn default() -> Self {
        Self {
            source: QueryNode::MatchAllDocs,
        }
    }
}

impl FromStr for DatadogSearchConfig {
    type Err = <QueryNode as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(|source| Self { source })
    }
}

impl From<QueryNode> for DatadogSearchConfig {
    fn from(source: QueryNode) -> Self {
        Self { source }
    }
}

impl_generate_config_from_default!(DatadogSearchConfig);

/// Runner that contains the boxed `Matcher` function to check whether an `Event` matches
/// a [Datadog Search Syntax query](https://docs.datadoghq.com/logs/explorer/search_syntax/).
#[derive(Debug, Clone)]
pub struct DatadogSearchRunner {
    matcher: Box<dyn Matcher<Event>>,
}

impl Conditional for DatadogSearchRunner {
    fn check(&self, e: Event) -> (bool, Event) {
        let result = self.matcher.run(&e);
        (result, e)
    }
}

impl ConditionalConfig for DatadogSearchConfig {
    fn build(
        &self,
        _enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) -> crate::Result<Condition> {
        let matcher = as_log(build_matcher(&self.source, &EventFilter).map_err(|e| e.to_string())?);

        Ok(Condition::DatadogSearch(DatadogSearchRunner { matcher }))
    }
}

/// Run the provided `Matcher` when we're dealing with `LogEvent`s. Otherwise, return false.
fn as_log(matcher: Box<dyn Matcher<LogEvent>>) -> Box<dyn Matcher<Event>> {
    Run::boxed(move |ev| match ev {
        Event::Log(log) => matcher.run(log),
        _ => false,
    })
}

#[derive(Default, Clone)]
struct EventFilter;

/// Uses the default `Resolver`, to build a `Vec<Field>`.
impl Resolver for EventFilter {}

impl Filter<LogEvent> for EventFilter {
    fn exists(&self, field: Field) -> Result<Box<dyn Matcher<LogEvent>>, PathParseError> {
        Ok(match field {
            Field::Tag(tag) => {
                let starts_with = format!("{tag}:");

                any_string_match_multiple(vec!["ddtags", "tags"], move |value| {
                    value == tag || value.starts_with(&starts_with)
                })
            }
            // Literal field 'tags' needs to be compared by key.
            Field::Reserved(field) if field == "tags" => {
                any_string_match_multiple(vec!["ddtags", "tags"], move |value| value == field)
            }
            // A literal "source" field should string match in "source" and "ddsource" fields (OR condition).
            Field::Reserved(field) if field == "source" => {
                exists_match_multiple(vec!["ddsource", "source"])
            }

            Field::Default(f) | Field::Attribute(f) | Field::Reserved(f) => exists_match(f),
        })
    }

    fn equals(
        &self,
        field: Field,
        to_match: &str,
    ) -> Result<Box<dyn Matcher<LogEvent>>, PathParseError> {
        Ok(match field {
            // Default fields are compared by word boundary.
            Field::Default(field) => {
                let re = word_regex(to_match);

                string_match(&field, move |value| re.is_match(&value))
            }
            // A literal "tags" field should match by key.
            Field::Reserved(field) if field == "tags" => {
                let to_match = to_match.to_owned();

                array_match_multiple(vec!["ddtags", "tags"], move |values| {
                    values.contains(&Value::Bytes(Bytes::copy_from_slice(to_match.as_bytes())))
                })
            }
            // Individual tags are compared by element key:value.
            Field::Tag(tag) => {
                let value_bytes = Value::Bytes(format!("{tag}:{to_match}").into());

                array_match_multiple(vec!["ddtags", "tags"], move |values| {
                    values.contains(&value_bytes)
                })
            }
            // A literal "source" field should string match in "source" and "ddsource" fields (OR condition).
            Field::Reserved(field) if field == "source" => {
                let to_match = to_match.to_owned();

                string_match_multiple(vec!["ddsource", "source"], move |value| value == to_match)
            }
            // Reserved values are matched by string equality.
            Field::Reserved(field) => {
                let to_match = to_match.to_owned();

                string_match(field, move |value| value == to_match)
            }
            // Attribute values can be strings or numeric types
            Field::Attribute(field) => {
                let to_match = to_match.to_owned();

                simple_scalar_match(field, move |value| value == to_match)
            }
        })
    }

    fn prefix(
        &self,
        field: Field,
        prefix: &str,
    ) -> Result<Box<dyn Matcher<LogEvent>>, PathParseError> {
        Ok(match field {
            // Default fields are matched by word boundary.
            Field::Default(field) => {
                let re = word_regex(&format!("{prefix}*"));

                string_match(field, move |value| re.is_match(&value))
            }
            // Tags are recursed until a match is found.
            Field::Tag(tag) => {
                let starts_with = format!("{tag}:{prefix}");

                any_string_match_multiple(vec!["ddtags", "tags"], move |value| {
                    value.starts_with(&starts_with)
                })
            }
            // A literal "source" field should string match in "source" and "ddsource" fields (OR condition).
            Field::Reserved(field) if field == "source" => {
                let prefix = prefix.to_owned();

                string_match_multiple(vec!["ddsource", "source"], move |value| {
                    value.starts_with(&prefix)
                })
            }

            // All other field types are compared by complete value.
            Field::Reserved(field) | Field::Attribute(field) => {
                let prefix = prefix.to_owned();

                string_match(field, move |value| value.starts_with(&prefix))
            }
        })
    }

    fn wildcard(
        &self,
        field: Field,
        wildcard: &str,
    ) -> Result<Box<dyn Matcher<LogEvent>>, PathParseError> {
        Ok(match field {
            Field::Default(field) => {
                let re = word_regex(wildcard);

                string_match(field, move |value| re.is_match(&value))
            }
            Field::Tag(tag) => {
                let re = wildcard_regex(&format!("{tag}:{wildcard}"));

                any_string_match_multiple(vec!["ddtags", "tags"], move |value| re.is_match(&value))
            }
            // A literal "source" field should string match in "source" and "ddsource" fields (OR condition).
            Field::Reserved(field) if field == "source" => {
                let re = wildcard_regex(wildcard);

                string_match_multiple(vec!["ddsource", "source"], move |value| re.is_match(&value))
            }
            Field::Reserved(field) | Field::Attribute(field) => {
                let re = wildcard_regex(wildcard);

                string_match(field, move |value| re.is_match(&value))
            }
        })
    }

    fn compare(
        &self,
        field: Field,
        comparator: Comparison,
        comparison_value: ComparisonValue,
    ) -> Result<Box<dyn Matcher<LogEvent>>, PathParseError> {
        let rhs = Cow::from(comparison_value.to_string());

        Ok(match field {
            // Attributes are compared numerically if the value is numeric, or as strings otherwise.
            Field::Attribute(f) => {
                Run::boxed(move |log: &LogEvent| {
                    match (
                        log.parse_path_and_get_value(f.as_str()).ok().flatten(),
                        &comparison_value,
                    ) {
                        // Integers.
                        (Some(Value::Integer(lhs)), ComparisonValue::Integer(rhs)) => {
                            match comparator {
                                Comparison::Lt => lhs < rhs,
                                Comparison::Lte => lhs <= rhs,
                                Comparison::Gt => lhs > rhs,
                                Comparison::Gte => lhs >= rhs,
                            }
                        }
                        // Integer value - Float boundary
                        (Some(Value::Integer(lhs)), ComparisonValue::Float(rhs)) => {
                            match comparator {
                                Comparison::Lt => (*lhs as f64) < *rhs,
                                Comparison::Lte => *lhs as f64 <= *rhs,
                                Comparison::Gt => *lhs as f64 > *rhs,
                                Comparison::Gte => *lhs as f64 >= *rhs,
                            }
                        }
                        // Floats.
                        (Some(Value::Float(lhs)), ComparisonValue::Float(rhs)) => {
                            match comparator {
                                Comparison::Lt => lhs.into_inner() < *rhs,
                                Comparison::Lte => lhs.into_inner() <= *rhs,
                                Comparison::Gt => lhs.into_inner() > *rhs,
                                Comparison::Gte => lhs.into_inner() >= *rhs,
                            }
                        }
                        // Float value - Integer boundary
                        (Some(Value::Float(lhs)), ComparisonValue::Integer(rhs)) => {
                            match comparator {
                                Comparison::Lt => lhs.into_inner() < *rhs as f64,
                                Comparison::Lte => lhs.into_inner() <= *rhs as f64,
                                Comparison::Gt => lhs.into_inner() > *rhs as f64,
                                Comparison::Gte => lhs.into_inner() >= *rhs as f64,
                            }
                        }
                        // Where the rhs is a string ref, the lhs is coerced into a string.
                        (Some(Value::Bytes(v)), ComparisonValue::String(rhs)) => {
                            let lhs = String::from_utf8_lossy(v);
                            let rhs = Cow::from(rhs);

                            match comparator {
                                Comparison::Lt => lhs < rhs,
                                Comparison::Lte => lhs <= rhs,
                                Comparison::Gt => lhs > rhs,
                                Comparison::Gte => lhs >= rhs,
                            }
                        }
                        // Otherwise, compare directly as strings.
                        (Some(Value::Bytes(v)), _) => {
                            let lhs = String::from_utf8_lossy(v);

                            match comparator {
                                Comparison::Lt => lhs < rhs,
                                Comparison::Lte => lhs <= rhs,
                                Comparison::Gt => lhs > rhs,
                                Comparison::Gte => lhs >= rhs,
                            }
                        }
                        _ => false,
                    }
                })
            }
            // Tag values need extracting by "key:value" to be compared.
            Field::Tag(tag) => any_string_match_multiple(vec!["ddtags", "tags"], move |value| {
                match value.split_once(':') {
                    Some((t, lhs)) if t == tag => {
                        let lhs = Cow::from(lhs);

                        match comparator {
                            Comparison::Lt => lhs < rhs,
                            Comparison::Lte => lhs <= rhs,
                            Comparison::Gt => lhs > rhs,
                            Comparison::Gte => lhs >= rhs,
                        }
                    }
                    _ => false,
                }
            }),
            // A literal "source" field should string match in "source" and "ddsource" fields (OR condition).
            Field::Reserved(field) if field == "source" => {
                string_match_multiple(vec!["ddsource", "source"], move |lhs| match comparator {
                    Comparison::Lt => lhs < rhs,
                    Comparison::Lte => lhs <= rhs,
                    Comparison::Gt => lhs > rhs,
                    Comparison::Gte => lhs >= rhs,
                })
            }
            // All other tag types are compared by string.
            Field::Default(field) | Field::Reserved(field) => {
                string_match(field, move |lhs| match comparator {
                    Comparison::Lt => lhs < rhs,
                    Comparison::Lte => lhs <= rhs,
                    Comparison::Gt => lhs > rhs,
                    Comparison::Gte => lhs >= rhs,
                })
            }
        })
    }
}

// Returns a `Matcher` that returns true if the field exists.
fn exists_match<S>(field: S) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String>,
{
    let field = field.into();

    Run::boxed(move |log: &LogEvent| {
        log.parse_path_and_get_value(field.as_str())
            .ok()
            .flatten()
            .is_some()
    })
}

/// Returns a `Matcher` that returns true if the field resolves to a string,
/// numeric, or boolean which matches the provided `func`.
fn simple_scalar_match<S, F>(field: S, func: F) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String>,
    F: Fn(Cow<str>) -> bool + Send + Sync + Clone + 'static,
{
    let field = field.into();

    Run::boxed(move |log: &LogEvent| {
        match log.parse_path_and_get_value(field.as_str()).ok().flatten() {
            Some(Value::Boolean(v)) => func(v.to_string().into()),
            Some(Value::Bytes(v)) => func(String::from_utf8_lossy(v)),
            Some(Value::Integer(v)) => func(v.to_string().into()),
            Some(Value::Float(v)) => func(v.to_string().into()),
            _ => false,
        }
    })
}

/// Returns a `Matcher` that returns true if the field resolves to a string which
/// matches the provided `func`.
fn string_match<S, F>(field: S, func: F) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String>,
    F: Fn(Cow<str>) -> bool + Send + Sync + Clone + 'static,
{
    let field = field.into();

    Run::boxed(move |log: &LogEvent| {
        match log.parse_path_and_get_value(field.as_str()).ok().flatten() {
            Some(Value::Bytes(v)) => func(String::from_utf8_lossy(v)),
            _ => false,
        }
    })
}

// Returns a `Matcher` that returns true if any provided field exists.
fn exists_match_multiple<S>(fields: Vec<S>) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String> + Clone + Send + Sync + 'static,
{
    Run::boxed(move |log: &LogEvent| {
        fields
            .iter()
            .any(|field| exists_match(field.clone()).run(log))
    })
}

/// Returns a `Matcher` that returns true if any provided field resolves to a string which
/// matches the provided `func`.
fn string_match_multiple<S, F>(fields: Vec<S>, func: F) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String> + Clone + Send + Sync + 'static,
    F: Fn(Cow<str>) -> bool + Send + Sync + Clone + 'static,
{
    Run::boxed(move |log: &LogEvent| {
        fields
            .iter()
            .any(|field| string_match(field.clone(), func.clone()).run(log))
    })
}

fn any_string_match_multiple<S, F>(fields: Vec<S>, func: F) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String> + Clone + Send + Sync + 'static,
    F: Fn(Cow<str>) -> bool + Send + Sync + Clone + 'static,
{
    any_match_multiple(fields, move |value| {
        let bytes = value.coerce_to_bytes();
        func(String::from_utf8_lossy(&bytes))
    })
}

/// Returns a `Matcher` that returns true if any provided field of the log event resolves to an array, where
/// at least one `Value` it contains matches the provided `func`.
fn any_match_multiple<S, F>(fields: Vec<S>, func: F) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String> + Clone + Send + Sync + 'static,
    F: Fn(&Value) -> bool + Send + Sync + Clone + 'static,
{
    array_match_multiple(fields, move |values| values.iter().any(&func))
}

/// Returns a `Matcher` that returns true if any provided field of the log event resolves to an array, where
/// the vector of `Value`s the array contains matches the provided `func`.
fn array_match_multiple<S, F>(fields: Vec<S>, func: F) -> Box<dyn Matcher<LogEvent>>
where
    S: Into<String> + Clone + Send + Sync + 'static,
    F: Fn(&Vec<Value>) -> bool + Send + Sync + Clone + 'static,
{
    Run::boxed(move |log: &LogEvent| {
        fields.iter().any(|field| {
            let field = field.clone().into();
            match log.parse_path_and_get_value(field.as_str()).ok().flatten() {
                Some(Value::Array(values)) => func(values),
                _ => false,
            }
        })
    })
}

