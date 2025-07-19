use super::all_fields;
use crate::event::{KeyString, ObjectMap};

/// Iterates over all paths in form `a.b[0].c[1]` in alphabetical order.
/// It is implemented as a wrapper around `all_fields` to reduce code
/// duplication.
pub fn keys(fields: &ObjectMap) -> impl Iterator<Item = KeyString> + '_ {
    all_fields(fields).map(|(k, _)| k)
}

