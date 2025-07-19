use vector_config::configurable_component;

/// Per metric set expiration options.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PerMetricSetExpiration {
    /// Metric name to apply this expiration to. Ignores metric name if not defined.
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub name: Option<MetricNameMatcherConfig>,
    /// Labels to apply this expiration to. Ignores labels if not defined.
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(
        docs::enum_tag_field = "type",
        docs::enum_tagging = "internal",
        docs::enum_tag_description = "Metric label matcher type."
    ))]
    pub labels: Option<MetricLabelMatcherConfig>,
    /// The amount of time, in seconds, that internal metrics will persist after having not been
    /// updated before they expire and are removed.
    ///
    /// Set this to a value larger than your `internal_metrics` scrape interval (default 5 minutes)
    /// so that metrics live long enough to be emitted and captured.
    #[configurable(metadata(docs::examples = 60.0))]
    pub expire_secs: f64,
}

/// Configuration for metric name matcher.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "Metric name matcher type."))]
pub enum MetricNameMatcherConfig {
    /// Only considers exact name matches.
    Exact {
        /// The exact metric name.
        value: String,
    },
    /// Compares metric name to the provided pattern.
    Regex {
        /// Pattern to compare to.
        pattern: String,
    },
}

/// Configuration for metric labels matcher.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "Metric label matcher type."))]
pub enum MetricLabelMatcher {
    /// Looks for an exact match of one label key value pair.
    Exact {
        /// Metric key to look for.
        key: String,
        /// The exact metric label value.
        value: String,
    },
    /// Compares label value with given key to the provided pattern.
    Regex {
        /// Metric key to look for.
        key: String,
        /// Pattern to compare metric label value to.
        value_pattern: String,
    },
}

/// Configuration for metric labels matcher group.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "Metric label group matcher type."))]
pub enum MetricLabelMatcherConfig {
    /// Checks that any of the provided matchers can be applied to given metric.
    Any {
        /// List of matchers to check.
        matchers: Vec<MetricLabelMatcher>,
    },
    /// Checks that all of the provided matchers can be applied to given metric.
    All {
        /// List of matchers to check.
        matchers: Vec<MetricLabelMatcher>,
    },
}

/// Tests to confirm complex examples configuration
