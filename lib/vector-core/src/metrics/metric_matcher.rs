use std::time::Duration;

use metrics::Key;
use regex::Regex;

use crate::config::metrics_expiration::{
    MetricLabelMatcher, MetricLabelMatcherConfig, MetricNameMatcherConfig, PerMetricSetExpiration,
};

use super::recency::KeyMatcher;

pub(super) struct MetricKeyMatcher {
    name: Option<MetricNameMatcher>,
    labels: Option<LabelsMatcher>,
}

impl KeyMatcher<Key> for MetricKeyMatcher {
    fn matches(&self, key: &Key) -> bool {
        let name_match = self.name.as_ref().is_none_or(|m| m.matches(key));
        let labels_match = self.labels.as_ref().is_none_or(|l| l.matches(key));
        name_match && labels_match
    }
}

impl TryFrom<PerMetricSetExpiration> for MetricKeyMatcher {
    type Error = super::Error;

    fn try_from(value: PerMetricSetExpiration) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.map(TryInto::try_into).transpose()?,
            labels: value.labels.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<PerMetricSetExpiration> for (MetricKeyMatcher, Duration) {
    type Error = super::Error;

    fn try_from(value: PerMetricSetExpiration) -> Result<Self, Self::Error> {
        if value.expire_secs <= 0.0 {
            return Err(super::Error::TimeoutMustBePositive {
                timeout: value.expire_secs,
            });
        }
        let duration = Duration::from_secs_f64(value.expire_secs);
        Ok((value.try_into()?, duration))
    }
}

enum MetricNameMatcher {
    Exact(String),
    Regex(Regex),
}

impl KeyMatcher<Key> for MetricNameMatcher {
    fn matches(&self, key: &Key) -> bool {
        match self {
            MetricNameMatcher::Exact(name) => key.name() == name,
            MetricNameMatcher::Regex(regex) => regex.is_match(key.name()),
        }
    }
}

impl TryFrom<MetricNameMatcherConfig> for MetricNameMatcher {
    type Error = super::Error;

    fn try_from(value: MetricNameMatcherConfig) -> Result<Self, Self::Error> {
        Ok(match value {
            MetricNameMatcherConfig::Exact { value } => MetricNameMatcher::Exact(value),
            MetricNameMatcherConfig::Regex { pattern } => MetricNameMatcher::Regex(
                Regex::new(&pattern).map_err(|_| super::Error::InvalidRegexPattern { pattern })?,
            ),
        })
    }
}

enum LabelsMatcher {
    Any(Vec<LabelsMatcher>),
    All(Vec<LabelsMatcher>),
    Exact(String, String),
    Regex(String, Regex),
}

impl KeyMatcher<Key> for LabelsMatcher {
    fn matches(&self, key: &Key) -> bool {
        match self {
            LabelsMatcher::Any(vec) => vec.iter().any(|m| m.matches(key)),
            LabelsMatcher::All(vec) => vec.iter().all(|m| m.matches(key)),
            LabelsMatcher::Exact(label_key, label_value) => key
                .labels()
                .any(|l| l.key() == label_key && l.value() == label_value),
            LabelsMatcher::Regex(label_key, regex) => key
                .labels()
                .any(|l| l.key() == label_key && regex.is_match(l.value())),
        }
    }
}

impl TryFrom<MetricLabelMatcher> for LabelsMatcher {
    type Error = super::Error;

    fn try_from(value: MetricLabelMatcher) -> Result<Self, Self::Error> {
        Ok(match value {
            MetricLabelMatcher::Exact { key, value } => Self::Exact(key, value),
            MetricLabelMatcher::Regex { key, value_pattern } => Self::Regex(
                key,
                Regex::new(&value_pattern).map_err(|_| super::Error::InvalidRegexPattern {
                    pattern: value_pattern,
                })?,
            ),
        })
    }
}

impl TryFrom<MetricLabelMatcherConfig> for LabelsMatcher {
    type Error = super::Error;

    fn try_from(value: MetricLabelMatcherConfig) -> Result<Self, Self::Error> {
        Ok(match value {
            MetricLabelMatcherConfig::Any { matchers } => Self::Any(
                matchers
                    .into_iter()
                    .map(TryInto::<LabelsMatcher>::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            MetricLabelMatcherConfig::All { matchers } => Self::All(
                matchers
                    .into_iter()
                    .map(TryInto::<LabelsMatcher>::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        })
    }
}

