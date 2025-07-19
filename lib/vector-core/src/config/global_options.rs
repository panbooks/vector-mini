use std::{fs::DirBuilder, path::PathBuf, time::Duration};

use snafu::{ResultExt, Snafu};
use vector_common::TimeZone;
use vector_config::{configurable_component, impl_generate_config_from_default};

use super::super::default_data_dir;
use super::metrics_expiration::PerMetricSetExpiration;
use super::Telemetry;
use super::{proxy::ProxyConfig, AcknowledgementsConfig, LogSchema};
use crate::serde::bool_or_struct;

#[derive(Debug, Snafu)]
pub(crate) enum DataDirError {
    #[snafu(display("data_dir option required, but not given here or globally"))]
    MissingDataDir,
    #[snafu(display("data_dir {:?} does not exist", data_dir))]
    DoesNotExist { data_dir: PathBuf },
    #[snafu(display("data_dir {:?} is not writable", data_dir))]
    NotWritable { data_dir: PathBuf },
    #[snafu(display(
        "Could not create subdirectory {:?} inside of data dir {:?}: {}",
        subdir,
        data_dir,
        source
    ))]
    CouldNotCreate {
        subdir: PathBuf,
        data_dir: PathBuf,
        source: std::io::Error,
    },
}

/// Specifies the wildcard matching mode, relaxed allows configurations where wildcard doesn not match any existing inputs
#[configurable_component]
#[derive(Clone, Debug, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WildcardMatching {
    /// Strict matching (must match at least one existing input)
    #[default]
    Strict,

    /// Relaxed matching (must match 0 or more inputs)
    Relaxed,
}

/// Global configuration options.
//
// If this is modified, make sure those changes are reflected in the `ConfigBuilder::append`
// function!
#[configurable_component(global_option("global_option"))]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GlobalOptions {
    /// The directory used for persisting Vector state data.
    ///
    /// This is the directory where Vector will store any state data, such as disk buffers, file
    /// checkpoints, and more.
    ///
    /// Vector must have write permissions to this directory.
    #[serde(default = "crate::default_data_dir")]
    #[configurable(metadata(docs::common = false))]
    pub data_dir: Option<PathBuf>,

    /// Set wildcard matching mode for inputs
    ///
    /// Setting this to "relaxed" allows configurations with wildcards that do not match any inputs
    /// to be accepted without causing an error.
    #[serde(skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub wildcard_matching: Option<WildcardMatching>,

    /// Default log schema for all events.
    ///
    /// This is used if a component does not have its own specific log schema. All events use a log
    /// schema, whether or not the default is used, to assign event fields on incoming events.
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub log_schema: LogSchema,

    /// Telemetry options.
    ///
    /// Determines whether `source` and `service` tags should be emitted with the
    /// `component_sent_*` and `component_received_*` events.
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub telemetry: Telemetry,

    /// The name of the time zone to apply to timestamp conversions that do not contain an explicit time zone.
    ///
    /// The time zone name may be any name in the [TZ database][tzdb] or `local` to indicate system
    /// local time.
    ///
    /// Note that in Vector/VRL all timestamps are represented in UTC.
    ///
    /// [tzdb]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::common = false))]
    pub timezone: Option<TimeZone>,

    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub proxy: ProxyConfig,

    /// Controls how acknowledgements are handled for all sinks by default.
    ///
    /// See [End-to-end Acknowledgements][e2e_acks] for more information on how Vector handles event
    /// acknowledgement.
    ///
    /// [e2e_acks]: https://vector.dev/docs/architecture/end-to-end-acknowledgements/
    #[serde(
        default,
        deserialize_with = "bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    #[configurable(metadata(docs::common = true, docs::required = false))]
    pub acknowledgements: AcknowledgementsConfig,

    /// The amount of time, in seconds, that internal metrics will persist after having not been
    /// updated before they expire and are removed.
    ///
    /// Deprecated: use expire_metrics_secs instead
    #[configurable(deprecated)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::hidden))]
    pub expire_metrics: Option<Duration>,

    /// The amount of time, in seconds, that internal metrics will persist after having not been
    /// updated before they expire and are removed.
    ///
    /// Set this to a value larger than your `internal_metrics` scrape interval (default 5 minutes)
    /// so metrics live long enough to be emitted and captured.
    #[serde(skip_serializing_if = "crate::serde::is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub expire_metrics_secs: Option<f64>,

    /// This allows configuring different expiration intervals for different metric sets.
    /// By default this is empty and any metric not matched by one of these sets will use
    /// the global default value, defined using `expire_metrics_secs`.
    #[serde(skip_serializing_if = "crate::serde::is_default")]
    pub expire_metrics_per_metric_set: Option<Vec<PerMetricSetExpiration>>,
}

impl_generate_config_from_default!(GlobalOptions);

impl GlobalOptions {
    /// Resolve the `data_dir` option in either the global or local config, and
    /// validate that it exists and is writable.
    ///
    /// # Errors
    ///
    /// Function will error if it is unable to make data directory.
    pub fn resolve_and_validate_data_dir(
        &self,
        local_data_dir: Option<&PathBuf>,
    ) -> crate::Result<PathBuf> {
        let data_dir = local_data_dir
            .or(self.data_dir.as_ref())
            .ok_or(DataDirError::MissingDataDir)
            .map_err(Box::new)?
            .clone();
        if !data_dir.exists() {
            return Err(DataDirError::DoesNotExist { data_dir }.into());
        }
        let readonly = std::fs::metadata(&data_dir)
            .map(|meta| meta.permissions().readonly())
            .unwrap_or(true);
        if readonly {
            return Err(DataDirError::NotWritable { data_dir }.into());
        }
        Ok(data_dir)
    }

    /// Resolve the `data_dir` option using `resolve_and_validate_data_dir` and
    /// then ensure a named subdirectory exists.
    ///
    /// # Errors
    ///
    /// Function will error if it is unable to make data subdirectory.
    pub fn resolve_and_make_data_subdir(
        &self,
        local: Option<&PathBuf>,
        subdir: &str,
    ) -> crate::Result<PathBuf> {
        let data_dir = self.resolve_and_validate_data_dir(local)?;

        let mut data_subdir = data_dir.clone();
        data_subdir.push(subdir);

        DirBuilder::new()
            .recursive(true)
            .create(&data_subdir)
            .with_context(|_| CouldNotCreateSnafu { subdir, data_dir })?;
        Ok(data_subdir)
    }

    /// Merge a second global configuration into self, and return the new merged data.
    ///
    /// # Errors
    ///
    /// Returns a list of textual errors if there is a merge conflict between the two global
    /// configs.
    pub fn merge(&self, with: Self) -> Result<Self, Vec<String>> {
        let mut errors = Vec::new();

        if conflicts(
            self.wildcard_matching.as_ref(),
            with.wildcard_matching.as_ref(),
        ) {
            errors.push("conflicting values for 'wildcard_matching' found".to_owned());
        }

        if conflicts(self.proxy.http.as_ref(), with.proxy.http.as_ref()) {
            errors.push("conflicting values for 'proxy.http' found".to_owned());
        }

        if conflicts(self.proxy.https.as_ref(), with.proxy.https.as_ref()) {
            errors.push("conflicting values for 'proxy.https' found".to_owned());
        }

        if !self.proxy.no_proxy.is_empty() && !with.proxy.no_proxy.is_empty() {
            errors.push("conflicting values for 'proxy.no_proxy' found".to_owned());
        }

        if conflicts(self.timezone.as_ref(), with.timezone.as_ref()) {
            errors.push("conflicting values for 'timezone' found".to_owned());
        }

        if conflicts(
            self.acknowledgements.enabled.as_ref(),
            with.acknowledgements.enabled.as_ref(),
        ) {
            errors.push("conflicting values for 'acknowledgements' found".to_owned());
        }

        if conflicts(self.expire_metrics.as_ref(), with.expire_metrics.as_ref()) {
            errors.push("conflicting values for 'expire_metrics' found".to_owned());
        }

        if conflicts(
            self.expire_metrics_secs.as_ref(),
            with.expire_metrics_secs.as_ref(),
        ) {
            errors.push("conflicting values for 'expire_metrics_secs' found".to_owned());
        }

        let data_dir = if self.data_dir.is_none() || self.data_dir == default_data_dir() {
            with.data_dir
        } else if with.data_dir != default_data_dir() && self.data_dir != with.data_dir {
            // If two configs both set 'data_dir' and have conflicting values
            // we consider this an error.
            errors.push("conflicting values for 'data_dir' found".to_owned());
            None
        } else {
            self.data_dir.clone()
        };

        // If the user has multiple config files, we must *merge* log schemas
        // until we meet a conflict, then we are allowed to error.
        let mut log_schema = self.log_schema.clone();
        if let Err(merge_errors) = log_schema.merge(&with.log_schema) {
            errors.extend(merge_errors);
        }

        let mut telemetry = self.telemetry.clone();
        telemetry.merge(&with.telemetry);

        let merged_expire_metrics_per_metric_set = match (
            &self.expire_metrics_per_metric_set,
            &with.expire_metrics_per_metric_set,
        ) {
            (Some(a), Some(b)) => Some(a.iter().chain(b).cloned().collect()),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };

        if errors.is_empty() {
            Ok(Self {
                data_dir,
                wildcard_matching: self.wildcard_matching.or(with.wildcard_matching),
                log_schema,
                telemetry,
                acknowledgements: self.acknowledgements.merge_default(&with.acknowledgements),
                timezone: self.timezone.or(with.timezone),
                proxy: self.proxy.merge(&with.proxy),
                expire_metrics: self.expire_metrics.or(with.expire_metrics),
                expire_metrics_secs: self.expire_metrics_secs.or(with.expire_metrics_secs),
                expire_metrics_per_metric_set: merged_expire_metrics_per_metric_set,
            })
        } else {
            Err(errors)
        }
    }

    /// Get the configured time zone, using "local" time if none is set.
    pub fn timezone(&self) -> TimeZone {
        self.timezone.unwrap_or(TimeZone::Local)
    }
}

fn conflicts<T: PartialEq>(this: Option<&T>, that: Option<&T>) -> bool {
    matches!((this, that), (Some(this), Some(that)) if this != that)
}

