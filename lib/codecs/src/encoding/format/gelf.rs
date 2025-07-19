use crate::gelf::GELF_TARGET_PATHS;
use crate::{gelf_fields::*, VALID_FIELD_REGEX};
use bytes::{BufMut, BytesMut};
use lookup::event_path;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio_util::codec::Encoder;
use vector_core::{
    config::{log_schema, DataType},
    event::{Event, KeyString, LogEvent, Value},
    schema,
};

/// On GELF encoding behavior:
///   Graylog has a relaxed parsing. They are much more lenient than the spec would
///   suggest. We've elected to take a more strict approach to maintain backwards compatibility
///   in the event that we need to change the behavior to be more relaxed, so that prior versions
///   of vector will still work.
///   The exception is that if 'Additional fields' are found to be missing an underscore prefix and
///   are otherwise valid field names, we prepend the underscore.
///
/// Errors that can occur during GELF serialization.
#[derive(Debug, Snafu)]
pub enum GelfSerializerError {
    #[snafu(display(r#"LogEvent does not contain required field: "{}""#, field))]
    MissingField { field: KeyString },
    #[snafu(display(
        r#"LogEvent contains field with invalid name not matching pattern '{}': "{}""#,
        pattern,
        field,
    ))]
    InvalidField { field: KeyString, pattern: String },
    #[snafu(display(
        r#"LogEvent contains a value with an invalid type. field = "{}" type = "{}" expected type = "{}""#,
        field,
        actual_type,
        expected_type
    ))]
    InvalidValueType {
        field: String,
        actual_type: String,
        expected_type: String,
    },
}

/// Config used to build a `GelfSerializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GelfSerializerConfig;

impl GelfSerializerConfig {
    /// Creates a new `GelfSerializerConfig`.
    pub const fn new() -> Self {
        Self
    }

    /// Build the `GelfSerializer` from this configuration.
    pub fn build(&self) -> GelfSerializer {
        GelfSerializer::new()
    }

    /// The data type of events that are accepted by `GelfSerializer`.
    pub fn input_type() -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement() -> schema::Requirement {
        // While technically we support `Value` variants that can't be losslessly serialized to
        // JSON, we don't want to enforce that limitation to users yet.
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the GELF format.
/// Spec: <https://docs.graylog.org/docs/gelf>
#[derive(Debug, Clone)]
pub struct GelfSerializer;

impl GelfSerializer {
    /// Creates a new `GelfSerializer`.
    pub fn new() -> Self {
        GelfSerializer
    }

    /// Encode event and represent it as JSON value.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, vector_common::Error> {
        // input_type() restricts the event type to LogEvents
        let log = to_gelf_event(event.into_log())?;
        serde_json::to_value(&log).map_err(|e| e.to_string().into())
    }
}

impl Default for GelfSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder<Event> for GelfSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let log = to_gelf_event(event.into_log())?;
        let writer = buffer.writer();
        serde_json::to_writer(writer, &log)?;
        Ok(())
    }
}

/// Returns Error for invalid type.
fn err_invalid_type(
    field: &str,
    expected_type: &str,
    actual_type: &str,
) -> vector_common::Result<()> {
    InvalidValueTypeSnafu {
        field,
        actual_type,
        expected_type,
    }
    .fail()
    .map_err(|e| e.to_string().into())
}

/// Validates that the GELF required fields exist in the event, coercing in some cases.
fn coerce_required_fields(mut log: LogEvent) -> vector_common::Result<LogEvent> {
    // returns Error for missing field
    fn err_missing_field(field: &str) -> vector_common::Result<()> {
        MissingFieldSnafu { field }
            .fail()
            .map_err(|e| e.to_string().into())
    }

    // add the VERSION if it does not exist
    if !log.contains(&GELF_TARGET_PATHS.version) {
        log.insert(&GELF_TARGET_PATHS.version, GELF_VERSION);
    }

    if !log.contains(&GELF_TARGET_PATHS.host) {
        err_missing_field(HOST)?;
    }

    if !log.contains(&GELF_TARGET_PATHS.short_message) {
        if let Some(message_key) = log_schema().message_key_target_path() {
            if log.contains(message_key) {
                log.rename_key(message_key, &GELF_TARGET_PATHS.short_message);
            } else {
                err_missing_field(SHORT_MESSAGE)?;
            }
        }
    }
    Ok(log)
}

/// Validates rules for field names and value types, coercing in some cases.
fn coerce_field_names_and_values(
    mut log: LogEvent,
) -> vector_common::Result<(LogEvent, Vec<String>)> {
    let mut missing_prefix = vec![];
    if let Some(event_data) = log.as_map_mut() {
        for (field, value) in event_data.iter_mut() {
            match field.as_str() {
                VERSION | HOST | SHORT_MESSAGE | FULL_MESSAGE | FACILITY | FILE => {
                    if !value.is_bytes() {
                        err_invalid_type(field, "UTF-8 string", value.kind_str())?;
                    }
                }
                TIMESTAMP => {
                    if !(value.is_timestamp() || value.is_integer()) {
                        err_invalid_type(field, "timestamp or integer", value.kind_str())?;
                    }

                    // convert a `Value::Timestamp` to a GELF specified timestamp where milliseconds are represented by the fractional part of a float.
                    if let Value::Timestamp(ts) = value {
                        let ts_millis = ts.timestamp_millis();
                        if ts_millis % 1000 != 0 {
                            *value = Value::Float(NotNan::new(ts_millis as f64 / 1000.0).unwrap());
                        } else {
                            // keep full range of representable time if no milliseconds are set
                            // but still convert to numeric according to GELF protocol
                            *value = Value::Integer(ts.timestamp())
                        }
                    }
                }
                LEVEL => {
                    if !value.is_integer() {
                        err_invalid_type(field, "integer", value.kind_str())?;
                    }
                }
                LINE => {
                    if !(value.is_float() || value.is_integer()) {
                        err_invalid_type(field, "number", value.kind_str())?;
                    }
                }
                _ => {
                    // additional fields must be only word chars, dashes and periods.
                    if !VALID_FIELD_REGEX.is_match(field) {
                        return InvalidFieldSnafu {
                            field: field.clone(),
                            pattern: VALID_FIELD_REGEX.to_string(),
                        }
                        .fail()
                        .map_err(|e| e.to_string().into());
                    }

                    // additional field values must be only strings or numbers
                    if !(value.is_integer() || value.is_float() || value.is_bytes()) {
                        err_invalid_type(field, "string or number", value.kind_str())?;
                    }

                    // Additional fields must be prefixed with underscores.
                    // Prepending the underscore since vector adds fields such as 'source_type'
                    // which would otherwise throw errors.
                    if !field.is_empty() && !field.starts_with('_') {
                        // flag the field as missing prefix to be modified later
                        missing_prefix.push(field.to_string());
                    }
                }
            }
        }
    }
    Ok((log, missing_prefix))
}

/// Validate if the input log event is valid GELF, potentially coercing the event into valid GELF.
fn to_gelf_event(log: LogEvent) -> vector_common::Result<LogEvent> {
    let log = coerce_required_fields(log).and_then(|log| {
        coerce_field_names_and_values(log).map(|(mut log, missing_prefix)| {
            // rename additional fields that were flagged as missing the underscore prefix
            for field in missing_prefix {
                log.rename_key(
                    event_path!(field.as_str()),
                    event_path!(format!("_{}", &field).as_str()),
                );
            }
            log
        })
    })?;

    Ok(log)
}

