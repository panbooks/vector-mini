use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use chrono::{DateTime, Timelike, Utc};
use ordered_float::NotNan;
use smallvec::SmallVec;
use vector_common::json_size::JsonSize;
use vrl::value::{KeyString, Value};

const NULL_SIZE: JsonSize = JsonSize::new(4);
const TRUE_SIZE: JsonSize = JsonSize::new(4);
const FALSE_SIZE: JsonSize = JsonSize::new(5);

const BRACKETS_SIZE: usize = 2;
const BRACES_SIZE: usize = 2;

const QUOTES_SIZE: usize = 2;
const COMMA_SIZE: usize = 1;
const COLON_SIZE: usize = 1;

const EPOCH_RFC3339_0: &str = "1970-01-01T00:00:00Z";
const EPOCH_RFC3339_3: &str = "1970-01-01T00:00:00.000Z";
const EPOCH_RFC3339_6: &str = "1970-01-01T00:00:00.000000Z";
const EPOCH_RFC3339_9: &str = "1970-01-01T00:00:00.000000000Z";

/// Return the estimated size of a type in bytes when encoded as JSON.
///
/// The result of this function is not guaranteed to be accurate but is intended to give a good
/// approximation to be used by internal events in Vector.
///
/// It should *NOT* be used for exact size calculations, as it may lead to incorrect results.
///
/// Implementers of this trait should strive to provide as accurate numbers as possible, without
/// introducing a significant performance penalty.
///
/// As an example, the size of a type that results in a JSON string should not iterate over
/// individual bytes of that string to check for the need of escape sequences or the need for UTF-8
/// REPLACEMENT CHARACTER, as those operations are too expensive to do. Instead, the size of the
/// string is the estimation of the actual size of the string in memory, combined with two
/// surrounding quotes.
///
/// Ideally, no allocations should take place in any implementation of this function.
pub trait EstimatedJsonEncodedSizeOf {
    fn estimated_json_encoded_size_of(&self) -> JsonSize;
}

impl<T: EstimatedJsonEncodedSizeOf> EstimatedJsonEncodedSizeOf for &T {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        T::estimated_json_encoded_size_of(self)
    }
}

impl<T: EstimatedJsonEncodedSizeOf> EstimatedJsonEncodedSizeOf for Option<T> {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match self {
            Some(v) => v.estimated_json_encoded_size_of(),
            None => NULL_SIZE,
        }
    }
}

impl<T: EstimatedJsonEncodedSizeOf, const N: usize> EstimatedJsonEncodedSizeOf
    for SmallVec<[T; N]>
{
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.iter().map(T::estimated_json_encoded_size_of).sum()
    }
}

impl EstimatedJsonEncodedSizeOf for Value {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match self {
            Value::Timestamp(v) => v.estimated_json_encoded_size_of(),
            Value::Object(v) => v.estimated_json_encoded_size_of(),
            Value::Array(v) => v.estimated_json_encoded_size_of(),
            Value::Bytes(v) => v.estimated_json_encoded_size_of(),
            Value::Regex(v) => v.as_str().estimated_json_encoded_size_of(),
            Value::Integer(v) => v.estimated_json_encoded_size_of(),
            Value::Float(v) => v.estimated_json_encoded_size_of(),
            Value::Boolean(v) => v.estimated_json_encoded_size_of(),
            Value::Null => NULL_SIZE,
        }
    }
}

/// For performance reasons, strings aren't checked for the need for escape characters, nor for the
/// need for UTF-8 replacement characters.
///
/// This is the main reason why `EstimatedJsonEncodedSizeOf` is named as is, as most other types can
/// be calculated exactly without a noticeable performance penalty.
impl EstimatedJsonEncodedSizeOf for str {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        JsonSize::new(QUOTES_SIZE + self.len())
    }
}

impl EstimatedJsonEncodedSizeOf for String {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.as_str().estimated_json_encoded_size_of()
    }
}

impl EstimatedJsonEncodedSizeOf for KeyString {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.as_str().estimated_json_encoded_size_of()
    }
}

impl EstimatedJsonEncodedSizeOf for Bytes {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        JsonSize::new(QUOTES_SIZE + self.len())
    }
}

impl EstimatedJsonEncodedSizeOf for bool {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        if *self {
            TRUE_SIZE
        } else {
            FALSE_SIZE
        }
    }
}

impl EstimatedJsonEncodedSizeOf for f64 {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        ryu::Buffer::new().format_finite(*self).len().into()
    }
}

impl EstimatedJsonEncodedSizeOf for f32 {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        ryu::Buffer::new().format_finite(*self).len().into()
    }
}

impl<T: EstimatedJsonEncodedSizeOf + Copy> EstimatedJsonEncodedSizeOf for NotNan<T> {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.into_inner().estimated_json_encoded_size_of()
    }
}

/// JSON only support string keys, so `K` is constrained to anything that can be converted into a
/// `str`.
impl<K, V> EstimatedJsonEncodedSizeOf for BTreeMap<K, V>
where
    K: AsRef<str>,
    V: EstimatedJsonEncodedSizeOf,
{
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let size = self.iter().fold(BRACES_SIZE, |acc, (k, v)| {
            acc + k.as_ref().estimated_json_encoded_size_of().get()
                + COLON_SIZE
                + v.estimated_json_encoded_size_of().get()
                + COMMA_SIZE
        });

        JsonSize::new(if size > BRACES_SIZE {
            size - COMMA_SIZE
        } else {
            size
        })
    }
}

/// JSON only support string keys, so `K` is constrained to anything that can be converted into a
/// `str`.
impl<K, V, S> EstimatedJsonEncodedSizeOf for HashMap<K, V, S>
where
    K: AsRef<str>,
    V: EstimatedJsonEncodedSizeOf,
    S: ::std::hash::BuildHasher,
{
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let size = self.iter().fold(BRACES_SIZE, |acc, (k, v)| {
            acc + k.as_ref().estimated_json_encoded_size_of().get()
                + COLON_SIZE
                + v.estimated_json_encoded_size_of().get()
                + COMMA_SIZE
        });

        JsonSize::new(if size > BRACES_SIZE {
            size - COMMA_SIZE
        } else {
            size
        })
    }
}

impl<V> EstimatedJsonEncodedSizeOf for Vec<V>
where
    V: EstimatedJsonEncodedSizeOf,
{
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let size = self.iter().fold(BRACKETS_SIZE, |acc, v| {
            acc + COMMA_SIZE + v.estimated_json_encoded_size_of().get()
        });

        JsonSize::new(if size > BRACKETS_SIZE {
            size - COMMA_SIZE
        } else {
            size
        })
    }
}

impl EstimatedJsonEncodedSizeOf for DateTime<Utc> {
    /// The timestamp is converted to a static epoch timestamp, to avoid any unnecessary
    /// allocations.
    ///
    /// The following invariants must hold for the size of timestamps to remain accurate:
    ///
    /// - `chrono::SecondsFormat::AutoSi` is used to calculate nanoseconds precision.
    /// - `use_z` is `true` for the `chrono::DateTime#to_rfc3339_opts` function call.
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let ns = self.nanosecond() % 1_000_000_000;
        let epoch = if ns == 0 {
            EPOCH_RFC3339_0
        } else if ns % 1_000_000 == 0 {
            EPOCH_RFC3339_3
        } else if ns % 1_000 == 0 {
            EPOCH_RFC3339_6
        } else {
            EPOCH_RFC3339_9
        };

        JsonSize::new(QUOTES_SIZE + epoch.len())
    }
}

impl EstimatedJsonEncodedSizeOf for u8 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // 0 ..= 255
        JsonSize::new(
            if        v <  10 { 1
            } else if v < 100 { 2
            } else            { 3 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for i8 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // -128 ..= 127
        JsonSize::new(
            if        v < -99 { 4
            } else if v <  -9 { 3
            } else if v <   0 { 2
            } else if v <  10 { 1
            } else if v < 100 { 2
            } else            { 3 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for u16 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // 0 ..= 65_535
        JsonSize::new(
            if        v <     10 { 1
            } else if v <    100 { 2
            } else if v <  1_000 { 3
            } else if v < 10_000 { 4
            } else               { 5 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for i16 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // -32_768 ..= 32_767
        JsonSize::new(
            if        v < -9_999 { 6
            } else if v <   -999 { 5
            } else if v <    -99 { 4
            } else if v <     -9 { 3
            } else if v <      0 { 2
            } else if v <     10 { 1
            } else if v <    100 { 2
            } else if v <  1_000 { 3
            } else if v < 10_000 { 4
            } else               { 5 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for u32 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // 0 ..= 4_294_967_295
        JsonSize::new(
            if        v <            10 { 1
            } else if v <           100 { 2
            } else if v <         1_000 { 3
            } else if v <        10_000 { 4
            } else if v <       100_000 { 5
            } else if v <     1_000_000 { 6
            } else if v <    10_000_000 { 7
            } else if v <   100_000_000 { 8
            } else if v < 1_000_000_000 { 9
            } else                      { 10 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for i32 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // -2_147_483_648 ..= 2_147_483_647
        JsonSize::new(
            if        v <  -999_999_999 { 11
            } else if v <   -99_999_999 { 10
            } else if v <    -9_999_999 {  9
            } else if v <      -999_999 {  8
            } else if v <       -99_999 {  7
            } else if v <        -9_999 {  6
            } else if v <          -999 {  5
            } else if v <           -99 {  4
            } else if v <            -9 {  3
            } else if v <             0 {  2
            } else if v <            10 {  1
            } else if v <           100 {  2
            } else if v <         1_000 {  3
            } else if v <        10_000 {  4
            } else if v <       100_000 {  5
            } else if v <     1_000_000 {  6
            } else if v <    10_000_000 {  7
            } else if v <   100_000_000 {  8
            } else if v < 1_000_000_000 {  9
            } else                      { 10 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for u64 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // 0 ..= 18_446_744_073_709_551_615
        JsonSize::new(
            if        v <                         10 {  1
            } else if v <                        100 {  2
            } else if v <                      1_000 {  3
            } else if v <                     10_000 {  4
            } else if v <                    100_000 {  5
            } else if v <                  1_000_000 {  6
            } else if v <                 10_000_000 {  7
            } else if v <                100_000_000 {  8
            } else if v <              1_000_000_000 {  9
            } else if v <             10_000_000_000 { 10
            } else if v <            100_000_000_000 { 11
            } else if v <          1_000_000_000_000 { 12
            } else if v <         10_000_000_000_000 { 13
            } else if v <        100_000_000_000_000 { 14
            } else if v <      1_000_000_000_000_000 { 15
            } else if v <     10_000_000_000_000_000 { 16
            } else if v <    100_000_000_000_000_000 { 17
            } else if v <  1_000_000_000_000_000_000 { 18
            } else if v < 10_000_000_000_000_000_000 { 19
            } else                                   { 20 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for i64 {
    #[rustfmt::skip]
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        let v = *self;

        // -9_223_372_036_854_775_808 ..= 9_223_372_036_854_775_807
        JsonSize::new(
            if        v <  -999_999_999_999_999_999 { 20
            } else if v <   -99_999_999_999_999_999 { 19
            } else if v <    -9_999_999_999_999_999 { 18
            } else if v <      -999_999_999_999_999 { 17
            } else if v <       -99_999_999_999_999 { 16
            } else if v <        -9_999_999_999_999 { 15
            } else if v <          -999_999_999_999 { 14
            } else if v <           -99_999_999_999 { 13
            } else if v <            -9_999_999_999 { 12
            } else if v <              -999_999_999 { 11
            } else if v <               -99_999_999 { 10
            } else if v <                -9_999_999 {  9
            } else if v <                  -999_999 {  8
            } else if v <                   -99_999 {  7
            } else if v <                    -9_999 {  6
            } else if v <                      -999 {  5
            } else if v <                       -99 {  4
            } else if v <                        -9 {  3
            } else if v <                         0 {  2
            } else if v <                        10 {  1
            } else if v <                       100 {  2
            } else if v <                     1_000 {  3
            } else if v <                    10_000 {  4
            } else if v <                   100_000 {  5
            } else if v <                 1_000_000 {  6
            } else if v <                10_000_000 {  7
            } else if v <               100_000_000 {  8
            } else if v <             1_000_000_000 {  9
            } else if v <            10_000_000_000 { 10
            } else if v <           100_000_000_000 { 11
            } else if v <         1_000_000_000_000 { 12
            } else if v <        10_000_000_000_000 { 13
            } else if v <       100_000_000_000_000 { 14
            } else if v <     1_000_000_000_000_000 { 15
            } else if v <    10_000_000_000_000_000 { 16
            } else if v <   100_000_000_000_000_000 { 17
            } else if v < 1_000_000_000_000_000_000 { 18
            } else                                  { 19 }
        )
    }
}

impl EstimatedJsonEncodedSizeOf for usize {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        (*self as u64).estimated_json_encoded_size_of()
    }
}

impl EstimatedJsonEncodedSizeOf for isize {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        (*self as i64).estimated_json_encoded_size_of()
    }
}

