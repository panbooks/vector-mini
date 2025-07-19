use std::{collections::HashMap, sync::LazyLock};

use regex::{Captures, Regex};

// Environment variable names can have any characters from the Portable Character Set other
// than NUL.  However, for Vector's interpolation, we are closer to what a shell supports which
// is solely of uppercase letters, digits, and the '_' (that is, the `[:word:]` regex class).
// In addition to these characters, we allow `.` as this commonly appears in environment
// variable names when they come from a Java properties file.
//
// https://pubs.opengroup.org/onlinepubs/000095399/basedefs/xbd_chap08.html
pub static ENVIRONMENT_VARIABLE_INTERPOLATION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        \$\$|
        \$([[:word:].]+)|
        \$\{([[:word:].]+)(?:(:?-|:?\?)([^}]*))?\}",
    )
    .unwrap()
});

/// Result<interpolated config, errors>
pub fn interpolate(input: &str, vars: &HashMap<String, String>) -> Result<String, Vec<String>> {
    let mut errors = Vec::new();

    let interpolated = ENVIRONMENT_VARIABLE_INTERPOLATION_REGEX
        .replace_all(input, |caps: &Captures<'_>| {
            let flags = caps.get(3).map(|m| m.as_str()).unwrap_or_default();
            let def_or_err = caps.get(4).map(|m| m.as_str()).unwrap_or_default();
            caps.get(1)
                .or_else(|| caps.get(2))
                .map(|m| m.as_str())
                .map(|name| {
                    let val = vars.get(name).map(|v| v.as_str());
                    match flags {
                        ":-" => match val {
                            Some(v) if !v.is_empty() => v,
                            _ => def_or_err,
                        },
                        "-" => val.unwrap_or(def_or_err),
                        ":?" => match val {
                            Some(v) if !v.is_empty() => v,
                            _ => {
                                errors.push(format!(
                                    "Non-empty environment variable required in config. name = {name:?}, error = {def_or_err:?}",
                                ));
                                ""
                            },
                        }
                        "?" => val.unwrap_or_else(|| {
                            errors.push(format!(
                                "Missing environment variable required in config. name = {name:?}, error = {def_or_err:?}",
                            ));
                            ""
                        }),
                        _ => val.unwrap_or_else(|| {
                            errors.push(format!(
                                "Missing environment variable in config. name = {name:?}",
                            ));
                            ""
                        }),
                    }
                })
                .unwrap_or("$")
                .to_string()
        })
        .into_owned();

    if errors.is_empty() {
        Ok(interpolated)
    } else {
        Err(errors)
    }
}

