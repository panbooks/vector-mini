//! Support for loading configs from multiple formats.

#![deny(missing_docs, missing_debug_implementations)]

use std::fmt;
use std::path::Path;
use std::str::FromStr;

use serde::{de, Deserialize, Serialize};
use vector_config_macros::Configurable;

/// A type alias to better capture the semantics.
pub type FormatHint = Option<Format>;

/// The format used to represent the configuration data.
#[derive(
    Debug,
    Default,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    Configurable,
)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// TOML format is used.
    #[default]
    Toml,
    /// JSON format is used.
    Json,
    /// YAML format is used.
    Yaml,
}

impl FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "toml" => Ok(Format::Toml),
            "yaml" => Ok(Format::Yaml),
            "json" => Ok(Format::Json),
            _ => Err(format!("Invalid format: {s}")),
        }
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let format = match self {
            Format::Toml => "toml",
            Format::Json => "json",
            Format::Yaml => "yaml",
        };
        write!(f, "{format}")
    }
}

impl Format {
    /// Obtain the format from the file path using extension as a hint.
    pub fn from_path<T: AsRef<Path>>(path: T) -> Result<Self, T> {
        match path.as_ref().extension().and_then(|ext| ext.to_str()) {
            Some("toml") => Ok(Format::Toml),
            Some("yaml") | Some("yml") => Ok(Format::Yaml),
            Some("json") => Ok(Format::Json),
            _ => Err(path),
        }
    }
}

/// Parse the string represented in the specified format.
pub fn deserialize<T>(content: &str, format: Format) -> Result<T, Vec<String>>
where
    T: de::DeserializeOwned,
{
    match format {
        Format::Toml => toml::from_str(content).map_err(|e| vec![e.to_string()]),
        Format::Yaml => serde_yaml::from_str::<serde_yaml::Value>(content)
            .and_then(|mut v| {
                v.apply_merge()?;
                serde_yaml::from_value(v)
            })
            .map_err(|e| vec![e.to_string()]),
        Format::Json => serde_json::from_str(content).map_err(|e| vec![e.to_string()]),
    }
}

/// Serialize the specified `value` into a string.
pub fn serialize<T>(value: &T, format: Format) -> Result<String, String>
where
    T: serde::ser::Serialize,
{
    match format {
        Format::Toml => toml::to_string(value).map_err(|e| e.to_string()),
        Format::Yaml => serde_yaml::to_string(value).map_err(|e| e.to_string()),
        Format::Json => serde_json::to_string_pretty(value).map_err(|e| e.to_string()),
    }
}

