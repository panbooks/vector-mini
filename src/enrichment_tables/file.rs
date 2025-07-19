//! Handles enrichment tables for `type = file`.
use std::{collections::HashMap, fs, hash::Hasher, path::PathBuf, time::SystemTime};

use bytes::Bytes;
use tracing::trace;
use vector_lib::configurable::configurable_component;
use vector_lib::enrichment::{Case, Condition, IndexHandle, Table};
use vector_lib::{conversion::Conversion, TimeZone};
use vrl::value::{ObjectMap, Value};

use crate::config::EnrichmentTableConfig;

/// File encoding configuration.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "File encoding type."))]
pub enum Encoding {
    /// Decodes the file as a [CSV][csv] (comma-separated values) file.
    ///
    /// [csv]: https://wikipedia.org/wiki/Comma-separated_values
    Csv {
        /// Whether or not the file contains column headers.
        ///
        /// When set to `true`, the first row of the CSV file will be read as the header row, and
        /// the values will be used for the names of each column. This is the default behavior.
        ///
        /// When set to `false`, columns are referred to by their numerical index.
        #[serde(default = "crate::serde::default_true")]
        include_headers: bool,

        /// The delimiter used to separate fields in each row of the CSV file.
        #[serde(default = "default_delimiter")]
        delimiter: char,
    },
}

impl Default for Encoding {
    fn default() -> Self {
        Self::Csv {
            include_headers: true,
            delimiter: default_delimiter(),
        }
    }
}

/// File-specific settings.
#[configurable_component]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileSettings {
    /// The path of the enrichment table file.
    ///
    /// Currently, only [CSV][csv] files are supported.
    ///
    /// [csv]: https://en.wikipedia.org/wiki/Comma-separated_values
    pub path: PathBuf,

    /// File encoding configuration.
    #[configurable(derived)]
    pub encoding: Encoding,
}

/// Configuration for the `file` enrichment table.
#[configurable_component(enrichment_table("file"))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileConfig {
    /// File-specific settings.
    #[configurable(derived)]
    pub file: FileSettings,

    /// Key/value pairs representing mapped log field names and types.
    ///
    /// This is used to coerce log fields from strings into their proper types. The available types are listed in the `Types` list below.
    ///
    /// Timestamp coercions need to be prefaced with `timestamp|`, for example `"timestamp|%F"`. Timestamp specifiers can use either of the following:
    ///
    /// 1. One of the built-in-formats listed in the `Timestamp Formats` table below.
    /// 2. The [time format specifiers][chrono_fmt] from Rust’s `chrono` library.
    ///
    /// Types
    ///
    /// - **`bool`**
    /// - **`string`**
    /// - **`float`**
    /// - **`integer`**
    /// - **`date`**
    /// - **`timestamp`** (see the table below for formats)
    ///
    /// Timestamp Formats
    ///
    /// | Format               | Description                                                                      | Example                          |
    /// |----------------------|----------------------------------------------------------------------------------|----------------------------------|
    /// | `%F %T`              | `YYYY-MM-DD HH:MM:SS`                                                            | `2020-12-01 02:37:54`            |
    /// | `%v %T`              | `DD-Mmm-YYYY HH:MM:SS`                                                           | `01-Dec-2020 02:37:54`           |
    /// | `%FT%T`              | [ISO 8601][iso8601]/[RFC 3339][rfc3339], without time zone                       | `2020-12-01T02:37:54`            |
    /// | `%FT%TZ`             | [ISO 8601][iso8601]/[RFC 3339][rfc3339], UTC                                     | `2020-12-01T09:37:54Z`           |
    /// | `%+`                 | [ISO 8601][iso8601]/[RFC 3339][rfc3339], UTC, with time zone                     | `2020-12-01T02:37:54-07:00`      |
    /// | `%a, %d %b %Y %T`    | [RFC 822][rfc822]/[RFC 2822][rfc2822], without time zone                         | `Tue, 01 Dec 2020 02:37:54`      |
    /// | `%a %b %e %T %Y`     | [ctime][ctime] format                                                            | `Tue Dec 1 02:37:54 2020`        |
    /// | `%s`                 | [UNIX timestamp][unix_ts]                                                        | `1606790274`                     |
    /// | `%a %d %b %T %Y`     | [date][date] command, without time zone                                          | `Tue 01 Dec 02:37:54 2020`       |
    /// | `%a %d %b %T %Z %Y`  | [date][date] command, with time zone                                             | `Tue 01 Dec 02:37:54 PST 2020`   |
    /// | `%a %d %b %T %z %Y`  | [date][date] command, with numeric time zone                                     | `Tue 01 Dec 02:37:54 -0700 2020` |
    /// | `%a %d %b %T %#z %Y` | [date][date] command, with numeric time zone (minutes can be missing or present) | `Tue 01 Dec 02:37:54 -07 2020`   |
    ///
    /// [date]: https://man7.org/linux/man-pages/man1/date.1.html
    /// [ctime]: https://www.cplusplus.com/reference/ctime
    /// [unix_ts]: https://en.wikipedia.org/wiki/Unix_time
    /// [rfc822]: https://tools.ietf.org/html/rfc822#section-5
    /// [rfc2822]: https://tools.ietf.org/html/rfc2822#section-3.3
    /// [iso8601]: https://en.wikipedia.org/wiki/ISO_8601
    /// [rfc3339]: https://tools.ietf.org/html/rfc3339
    /// [chrono_fmt]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "Represents mapped log field names and types."
    ))]
    pub schema: HashMap<String, String>,
}

const fn default_delimiter() -> char {
    ','
}

impl FileConfig {
    fn parse_column(
        &self,
        timezone: TimeZone,
        column: &str,
        row: usize,
        value: &str,
    ) -> Result<Value, String> {
        use chrono::TimeZone;

        Ok(match self.schema.get(column) {
            Some(format) => {
                let mut split = format.splitn(2, '|').map(|segment| segment.trim());

                match (split.next(), split.next()) {
                    (Some("date"), None) => Value::Timestamp(
                        chrono::FixedOffset::east_opt(0)
                            .expect("invalid timestamp")
                            .from_utc_datetime(
                                &chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                                    .map_err(|_| {
                                        format!("unable to parse date {value} found in row {row}")
                                    })?
                                    .and_hms_opt(0, 0, 0)
                                    .expect("invalid timestamp"),
                            )
                            .into(),
                    ),
                    (Some("date"), Some(format)) => Value::Timestamp(
                        chrono::FixedOffset::east_opt(0)
                            .expect("invalid timestamp")
                            .from_utc_datetime(
                                &chrono::NaiveDate::parse_from_str(value, format)
                                    .map_err(|_| {
                                        format!("unable to parse date {value} found in row {row}")
                                    })?
                                    .and_hms_opt(0, 0, 0)
                                    .expect("invalid timestamp"),
                            )
                            .into(),
                    ),
                    _ => {
                        let conversion =
                            Conversion::parse(format, timezone).map_err(|err| err.to_string())?;
                        conversion
                            .convert(Bytes::copy_from_slice(value.as_bytes()))
                            .map_err(|_| format!("unable to parse {value} found in row {row}"))?
                    }
                }
            }
            None => value.into(),
        })
    }

    /// Load the configured file into memory. Required to create a new file enrichment table.
    pub fn load_file(&self, timezone: TimeZone) -> crate::Result<FileData> {
        let Encoding::Csv {
            include_headers,
            delimiter,
        } = self.file.encoding;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(include_headers)
            .delimiter(delimiter as u8)
            .from_path(&self.file.path)?;

        let first_row = reader.records().next();
        let headers = if include_headers {
            reader
                .headers()?
                .iter()
                .map(|col| col.to_string())
                .collect::<Vec<_>>()
        } else {
            // If there are no headers in the datafile we make headers as the numerical index of
            // the column.
            match first_row {
                Some(Ok(ref row)) => (0..row.len()).map(|idx| idx.to_string()).collect(),
                _ => Vec::new(),
            }
        };

        let data = first_row
            .into_iter()
            .chain(reader.records())
            .map(|row| {
                Ok(row?
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| self.parse_column(timezone, &headers[idx], idx, col))
                    .collect::<Result<Vec<_>, String>>()?)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        trace!(
            "Loaded enrichment file {} with headers {:?}.",
            self.file.path.to_str().unwrap_or("path with invalid utf"),
            headers
        );

        let file = reader.into_inner();

        Ok(FileData {
            headers,
            data,
            modified: file.metadata()?.modified()?,
        })
    }
}

impl EnrichmentTableConfig for FileConfig {
    async fn build(
        &self,
        globals: &crate::config::GlobalOptions,
    ) -> crate::Result<Box<dyn Table + Send + Sync>> {
        Ok(Box::new(File::new(
            self.clone(),
            self.load_file(globals.timezone())?,
        )))
    }
}

impl_generate_config_from_default!(FileConfig);

/// The data resulting from loading a configured file.
pub struct FileData {
    /// The ordered set of headers of the data columns.
    pub headers: Vec<String>,
    /// The data contained in the file.
    pub data: Vec<Vec<Value>>,
    /// The last modified time of the file.
    pub modified: SystemTime,
}

/// A struct that implements [vector_lib::enrichment::Table] to handle loading enrichment data from a CSV file.
#[derive(Clone)]
pub struct File {
    config: FileConfig,
    last_modified: SystemTime,
    data: Vec<Vec<Value>>,
    headers: Vec<String>,
    indexes: Vec<(
        Case,
        Vec<usize>,
        HashMap<u64, Vec<usize>, hash_hasher::HashBuildHasher>,
    )>,
}

impl File {
    /// Creates a new [File] based on the provided config.
    pub fn new(config: FileConfig, data: FileData) -> Self {
        Self {
            config,
            last_modified: data.modified,
            data: data.data,
            headers: data.headers,
            indexes: Vec::new(),
        }
    }

    fn column_index(&self, col: &str) -> Option<usize> {
        self.headers.iter().position(|header| header == col)
    }

    /// Does the given row match all the conditions specified?
    fn row_equals(
        &self,
        case: Case,
        condition: &[Condition],
        row: &[Value],
        wildcard: Option<&Value>,
    ) -> bool {
        condition.iter().all(|condition| match condition {
            Condition::Equals { field, value } => match self.column_index(field) {
                None => false,
                Some(idx) => {
                    let current_row_value = &row[idx];

                    // Helper closure for comparing current_row_value with another value,
                    // respecting the specified case for Value::Bytes.
                    let compare_values = |val_to_compare: &Value| -> bool {
                        match (case, current_row_value, val_to_compare) {
                            (
                                Case::Insensitive,
                                Value::Bytes(bytes_row),
                                Value::Bytes(bytes_cmp),
                            ) => {
                                // Perform case-insensitive comparison for byte strings.
                                // If both are valid UTF-8, compare their lowercase versions.
                                // If both are non-UTF-8 bytes, compare them directly.
                                // If one is UTF-8 and the other is not, they are considered not equal.
                                match (
                                    std::str::from_utf8(bytes_row),
                                    std::str::from_utf8(bytes_cmp),
                                ) {
                                    (Ok(s_row), Ok(s_cmp)) => {
                                        s_row.to_lowercase() == s_cmp.to_lowercase()
                                    }
                                    (Err(_), Err(_)) => bytes_row == bytes_cmp,
                                    _ => false,
                                }
                            }
                            // For Case::Sensitive, or for Case::Insensitive with non-Bytes types,
                            // perform a direct equality check.
                            _ => current_row_value == val_to_compare,
                        }
                    };

                    // First, check if the row value matches the condition's value.
                    if compare_values(value) {
                        true
                    } else if let Some(wc_val) = wildcard {
                        // If not, and a wildcard is provided, check if the row value matches the wildcard.
                        compare_values(wc_val)
                    } else {
                        // Otherwise, no match.
                        false
                    }
                }
            },
            Condition::BetweenDates { field, from, to } => match self.column_index(field) {
                None => false,
                Some(idx) => match row[idx] {
                    Value::Timestamp(date) => from <= &date && &date <= to,
                    _ => false,
                },
            },
            Condition::FromDate { field, from } => match self.column_index(field) {
                None => false,
                Some(idx) => match row[idx] {
                    Value::Timestamp(date) => from <= &date,
                    _ => false,
                },
            },
            Condition::ToDate { field, to } => match self.column_index(field) {
                None => false,
                Some(idx) => match row[idx] {
                    Value::Timestamp(date) => &date <= to,
                    _ => false,
                },
            },
        })
    }

    fn add_columns(&self, select: Option<&[String]>, row: &[Value]) -> ObjectMap {
        self.headers
            .iter()
            .zip(row)
            .filter(|(header, _)| {
                select
                    .map(|select| select.contains(header))
                    // If no select is passed, we assume all columns are included
                    .unwrap_or(true)
            })
            .map(|(header, col)| (header.as_str().into(), col.clone()))
            .collect()
    }

    /// Order the fields in the index according to the position they are found in the header.
    fn normalize_index_fields(&self, index: &[&str]) -> Result<Vec<usize>, String> {
        // Get the positions of the fields we are indexing
        let normalized = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if index.contains(&col.as_ref()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if normalized.len() != index.len() {
            let missing = index
                .iter()
                .filter_map(|col| {
                    if self.headers.iter().any(|header| header == *col) {
                        None
                    } else {
                        Some(col.to_string())
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            Err(format!("field(s) '{missing}' missing from dataset"))
        } else {
            Ok(normalized)
        }
    }

    /// Creates an index with the given fields.
    /// Uses seahash to create a hash of the data that is used as the key in a hashmap lookup to
    /// the index of the row in the data.
    ///
    /// Ensure fields that are searched via a comparison are not included in the index!
    fn index_data(
        &self,
        fieldidx: &[usize],
        case: Case,
    ) -> Result<HashMap<u64, Vec<usize>, hash_hasher::HashBuildHasher>, String> {
        let mut index = HashMap::with_capacity_and_hasher(
            self.data.len(),
            hash_hasher::HashBuildHasher::default(),
        );

        for (idx, row) in self.data.iter().enumerate() {
            let mut hash = seahash::SeaHasher::default();

            for idx in fieldidx {
                hash_value(&mut hash, case, &row[*idx])?;
            }

            let key = hash.finish();

            let entry = index.entry(key).or_insert_with(Vec::new);
            entry.push(idx);
        }

        index.shrink_to_fit();

        Ok(index)
    }

    /// Sequentially searches through the iterator for the given condition.
    fn sequential<'a, I>(
        &'a self,
        data: I,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&'a Value>,
    ) -> impl Iterator<Item = ObjectMap> + 'a
    where
        I: Iterator<Item = &'a Vec<Value>> + 'a,
    {
        data.filter_map(move |row| {
            if self.row_equals(case, condition, row, wildcard) {
                Some(self.add_columns(select, row))
            } else {
                None
            }
        })
    }

    fn indexed<'a>(
        &'a self,
        case: Case,
        condition: &'a [Condition<'a>],
        handle: IndexHandle,
    ) -> Result<Option<&'a Vec<usize>>, String> {
        // The index to use has been passed, we can use this to search the data.
        // We are assuming that the caller has passed an index that represents the fields
        // being passed in the condition.
        let mut hash = seahash::SeaHasher::default();

        for header in self.headers.iter() {
            if let Some(Condition::Equals { value, .. }) = condition.iter().find(
                |condition| matches!(condition, Condition::Equals { field, .. } if field == header),
            ) {
                hash_value(&mut hash, case, value)?;
            }
        }

        let key = hash.finish();

        let IndexHandle(handle) = handle;
        Ok(self.indexes[handle].2.get(&key))
    }

    fn indexed_with_wildcard<'a>(
        &'a self,
        case: Case,
        wildcard: &'a Value,
        condition: &'a [Condition<'a>],
        handle: IndexHandle,
    ) -> Result<Option<&'a Vec<usize>>, String> {
        if let Some(result) = self.indexed(case, condition, handle)? {
            return Ok(Some(result));
        }

        // If lookup fails and a wildcard is provided, compute hash for the wildcard
        let mut wildcard_hash = seahash::SeaHasher::default();
        for header in self.headers.iter() {
            if condition.iter().any(
                |condition| matches!(condition, Condition::Equals { field, .. } if field == header),
            ) {
                hash_value(&mut wildcard_hash, case, wildcard)?;
            }
        }

        let wildcard_key = wildcard_hash.finish();
        let IndexHandle(handle) = handle;
        Ok(self.indexes[handle].2.get(&wildcard_key))
    }
}

/// Adds the bytes from the given value to the hash.
/// Each field is terminated by a `0` value to separate the fields
fn hash_value(hasher: &mut seahash::SeaHasher, case: Case, value: &Value) -> Result<(), String> {
    match value {
        Value::Bytes(bytes) => match case {
            Case::Sensitive => hasher.write(bytes),
            Case::Insensitive => hasher.write(
                std::str::from_utf8(bytes)
                    .map_err(|_| "column contains invalid utf".to_string())?
                    .to_lowercase()
                    .as_bytes(),
            ),
        },
        value => {
            let bytes: bytes::Bytes = value.encode_as_bytes()?;
            hasher.write(&bytes);
        }
    }

    hasher.write_u8(0);

    Ok(())
}

/// Returns an error if the iterator doesn't yield exactly one result.
fn single_or_err<I, T>(mut iter: T) -> Result<I, String>
where
    T: Iterator<Item = I>,
{
    let result = iter.next();

    if iter.next().is_some() {
        // More than one row has been found.
        Err("more than one row found".to_string())
    } else {
        result.ok_or_else(|| "no rows found".to_string())
    }
}

impl Table for File {
    fn find_table_row<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String> {
        match index {
            None => {
                // No index has been passed so we need to do a Sequential Scan.
                single_or_err(self.sequential(self.data.iter(), case, condition, select, wildcard))
            }
            Some(handle) => {
                let result = if let Some(wildcard) = wildcard {
                    self.indexed_with_wildcard(case, wildcard, condition, handle)?
                } else {
                    self.indexed(case, condition, handle)?
                }
                .ok_or_else(|| "no rows found in index".to_string())?
                .iter()
                .map(|idx| &self.data[*idx]);

                // Perform a sequential scan over the indexed result.
                single_or_err(self.sequential(result, case, condition, select, wildcard))
            }
        }
    }

    fn find_table_rows<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String> {
        match index {
            None => {
                // No index has been passed so we need to do a Sequential Scan.
                Ok(self
                    .sequential(self.data.iter(), case, condition, select, wildcard)
                    .collect())
            }
            Some(handle) => {
                // Perform a sequential scan over the indexed result.
                let indexed_result = if let Some(wildcard) = wildcard {
                    self.indexed_with_wildcard(case, wildcard, condition, handle)?
                } else {
                    self.indexed(case, condition, handle)?
                };

                Ok(self
                    .sequential(
                        indexed_result
                            .iter()
                            .flat_map(|results| results.iter().map(|idx| &self.data[*idx])),
                        case,
                        condition,
                        select,
                        wildcard,
                    )
                    .collect())
            }
        }
    }

    fn add_index(&mut self, case: Case, fields: &[&str]) -> Result<IndexHandle, String> {
        let normalized = self.normalize_index_fields(fields)?;
        match self
            .indexes
            .iter()
            .position(|index| index.0 == case && index.1 == normalized)
        {
            Some(pos) => {
                // This index already exists
                Ok(IndexHandle(pos))
            }
            None => {
                let index = self.index_data(&normalized, case)?;
                self.indexes.push((case, normalized, index));
                // The returned index handle is the position of the index in our list of indexes.
                Ok(IndexHandle(self.indexes.len() - 1))
            }
        }
    }

    /// Returns a list of the field names that are in each index
    fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
        self.indexes
            .iter()
            .map(|index| {
                let (case, fields, _) = index;
                (
                    *case,
                    fields
                        .iter()
                        .map(|idx| self.headers[*idx].clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    }

    /// Checks the modified timestamp of the data file to see if data has changed.
    fn needs_reload(&self) -> bool {
        matches!(fs::metadata(&self.config.file.path)
            .and_then(|metadata| metadata.modified()),
            Ok(modified) if modified > self.last_modified)
    }
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File {} row(s) {} index(es)",
            self.data.len(),
            self.indexes.len()
        )
    }
}

