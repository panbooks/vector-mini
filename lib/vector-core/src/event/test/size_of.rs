use std::mem;

use lookup::{path, PathPrefix};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use vector_common::byte_size_of::ByteSizeOf;

use super::common::Name;
use super::*;




//
// Log Events
//

/// The action that our model interpreter loop will take.
#[derive(Debug, Clone)]
pub(crate) enum Action {
    Contains {
        key: KeyString,
    },
    SizeOf,
    /// Insert a key/value pair into the [`LogEvent`]
    InsertFlat {
        key: KeyString,
        value: Value,
    },
    Remove {
        key: KeyString,
    },
}

impl Arbitrary for Action {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Action::InsertFlat {
                key: String::from(Name::arbitrary(g)).into(),
                value: Value::arbitrary(g),
            },
            1 => Action::SizeOf,
            2 => Action::Contains {
                key: String::from(Name::arbitrary(g)).into(),
            },
            3 => Action::Remove {
                key: String::from(Name::arbitrary(g)).into(),
            },
            _ => unreachable!(),
        }
    }
}

