use std::collections::HashSet;

use crate::event::{LogEvent, Value};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use dyn_clone::DynClone;
use ordered_float::NotNan;
use vector_lib::configurable::configurable_component;
use vrl::path::OwnedTargetPath;

/// Strategies for merging events.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "proptest", derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Discard all but the first value found.
    Discard,

    /// Discard all but the last value found.
    ///
    /// Works as a way to coalesce by not retaining `null`.
    Retain,

    /// Sum all numeric values.
    Sum,

    /// Keep the maximum numeric value seen.
    Max,

    /// Keep the minimum numeric value seen.
    Min,

    /// Append each value to an array.
    Array,

    /// Concatenate each string value, delimited with a space.
    Concat,

    /// Concatenate each string value, delimited with a newline.
    ConcatNewline,

    /// Concatenate each string, without a delimiter.
    ConcatRaw,

    /// Keep the shortest array seen.
    ShortestArray,

    /// Keep the longest array seen.
    LongestArray,

    /// Create a flattened array of all unique values.
    FlatUnique,
}

#[derive(Debug, Clone)]
struct DiscardMerger {
    v: Value,
}

impl DiscardMerger {
    const fn new(v: Value) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for DiscardMerger {
    fn add(&mut self, _v: Value) -> Result<(), String> {
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, self.v);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct RetainMerger {
    v: Value,
}

impl RetainMerger {
    #[allow(clippy::missing_const_for_fn)] // const cannot run destructor
    fn new(v: Value) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for RetainMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        if Value::Null != v {
            self.v = v;
        }
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, self.v);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ConcatMerger {
    v: BytesMut,
    join_by: Option<Vec<u8>>,
}

impl ConcatMerger {
    fn new(v: Bytes, join_by: Option<char>) -> Self {
        // We need to get the resulting bytes for this character in case it's actually a multi-byte character.
        let join_by = join_by.map(|c| c.to_string().into_bytes());

        Self {
            v: BytesMut::from(&v[..]),
            join_by,
        }
    }
}

impl ReduceValueMerger for ConcatMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        if let Value::Bytes(b) = v {
            if let Some(buf) = self.join_by.as_ref() {
                self.v.extend(&buf[..]);
            }
            self.v.extend_from_slice(&b);
            Ok(())
        } else {
            Err(format!(
                "expected string value, found: '{}'",
                v.to_string_lossy()
            ))
        }
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, Value::Bytes(self.v.into()));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ConcatArrayMerger {
    v: Vec<Value>,
}

impl ConcatArrayMerger {
    const fn new(v: Vec<Value>) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for ConcatArrayMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        if let Value::Array(a) = v {
            self.v.extend_from_slice(&a);
        } else {
            self.v.push(v);
        }
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, Value::Array(self.v));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ArrayMerger {
    v: Vec<Value>,
}

impl ArrayMerger {
    fn new(v: Value) -> Self {
        Self { v: vec![v] }
    }
}

impl ReduceValueMerger for ArrayMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        self.v.push(v);
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, Value::Array(self.v));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LongestArrayMerger {
    v: Vec<Value>,
}

impl LongestArrayMerger {
    const fn new(v: Vec<Value>) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for LongestArrayMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        if let Value::Array(a) = v {
            if a.len() > self.v.len() {
                self.v = a;
            }
            Ok(())
        } else {
            Err(format!(
                "expected array value, found: '{}'",
                v.to_string_lossy()
            ))
        }
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, Value::Array(self.v));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ShortestArrayMerger {
    v: Vec<Value>,
}

impl ShortestArrayMerger {
    const fn new(v: Vec<Value>) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for ShortestArrayMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        if let Value::Array(a) = v {
            if a.len() < self.v.len() {
                self.v = a;
            }
            Ok(())
        } else {
            Err(format!(
                "expected array value, found: '{}'",
                v.to_string_lossy()
            ))
        }
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, Value::Array(self.v));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct FlatUniqueMerger {
    v: HashSet<Value>,
}

#[allow(clippy::mutable_key_type)] // false positive due to bytes::Bytes
fn insert_value(h: &mut HashSet<Value>, v: Value) {
    match v {
        Value::Object(m) => {
            for (_, v) in m {
                h.insert(v);
            }
        }
        Value::Array(vec) => {
            for v in vec {
                h.insert(v);
            }
        }
        _ => {
            h.insert(v);
        }
    }
}

impl FlatUniqueMerger {
    #[allow(clippy::mutable_key_type)] // false positive due to bytes::Bytes
    fn new(v: Value) -> Self {
        let mut h = HashSet::default();
        insert_value(&mut h, v);
        Self { v: h }
    }
}

impl ReduceValueMerger for FlatUniqueMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        insert_value(&mut self.v, v);
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(path, Value::Array(self.v.into_iter().collect()));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TimestampWindowMerger {
    started: DateTime<Utc>,
    latest: DateTime<Utc>,
}

impl TimestampWindowMerger {
    const fn new(v: DateTime<Utc>) -> Self {
        Self {
            started: v,
            latest: v,
        }
    }
}

impl ReduceValueMerger for TimestampWindowMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        if let Value::Timestamp(ts) = v {
            self.latest = ts
        } else {
            return Err(format!(
                "expected timestamp value, found: {}",
                v.to_string_lossy()
            ));
        }
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        v.insert(
            format!("{path}_end").as_str(),
            Value::Timestamp(self.latest),
        );
        v.insert(path, Value::Timestamp(self.started));
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum NumberMergerValue {
    Int(i64),
    Float(NotNan<f64>),
}

impl From<i64> for NumberMergerValue {
    fn from(v: i64) -> Self {
        NumberMergerValue::Int(v)
    }
}

impl From<NotNan<f64>> for NumberMergerValue {
    fn from(v: NotNan<f64>) -> Self {
        NumberMergerValue::Float(v)
    }
}

#[derive(Debug, Clone)]
struct AddNumbersMerger {
    v: NumberMergerValue,
}

impl AddNumbersMerger {
    const fn new(v: NumberMergerValue) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for AddNumbersMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        // Try and keep max precision with integer values, but once we've
        // received a float downgrade to float precision.
        match v {
            Value::Integer(i) => match self.v {
                NumberMergerValue::Int(j) => self.v = NumberMergerValue::Int(i + j),
                NumberMergerValue::Float(j) => {
                    self.v = NumberMergerValue::Float(NotNan::new(i as f64).unwrap() + j)
                }
            },
            Value::Float(f) => match self.v {
                NumberMergerValue::Int(j) => self.v = NumberMergerValue::Float(f + j as f64),
                NumberMergerValue::Float(j) => self.v = NumberMergerValue::Float(f + j),
            },
            _ => {
                return Err(format!(
                    "expected numeric value, found: '{}'",
                    v.to_string_lossy()
                ));
            }
        }
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        match self.v {
            NumberMergerValue::Float(f) => v.insert(path, Value::Float(f)),
            NumberMergerValue::Int(i) => v.insert(path, Value::Integer(i)),
        };
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MaxNumberMerger {
    v: NumberMergerValue,
}

impl MaxNumberMerger {
    const fn new(v: NumberMergerValue) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for MaxNumberMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        // Try and keep max precision with integer values, but once we've
        // received a float downgrade to float precision.
        match v {
            Value::Integer(i) => {
                match self.v {
                    NumberMergerValue::Int(i2) => {
                        if i > i2 {
                            self.v = NumberMergerValue::Int(i);
                        }
                    }
                    NumberMergerValue::Float(f2) => {
                        let f = NotNan::new(i as f64).unwrap();
                        if f > f2 {
                            self.v = NumberMergerValue::Float(f);
                        }
                    }
                };
            }
            Value::Float(f) => {
                let f2 = match self.v {
                    NumberMergerValue::Int(i2) => NotNan::new(i2 as f64).unwrap(),
                    NumberMergerValue::Float(f2) => f2,
                };
                if f > f2 {
                    self.v = NumberMergerValue::Float(f);
                }
            }
            _ => {
                return Err(format!(
                    "expected numeric value, found: '{}'",
                    v.to_string_lossy()
                ));
            }
        }
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        match self.v {
            NumberMergerValue::Float(f) => v.insert(path, Value::Float(f)),
            NumberMergerValue::Int(i) => v.insert(path, Value::Integer(i)),
        };
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MinNumberMerger {
    v: NumberMergerValue,
}

impl MinNumberMerger {
    const fn new(v: NumberMergerValue) -> Self {
        Self { v }
    }
}

impl ReduceValueMerger for MinNumberMerger {
    fn add(&mut self, v: Value) -> Result<(), String> {
        // Try and keep max precision with integer values, but once we've
        // received a float downgrade to float precision.
        match v {
            Value::Integer(i) => {
                match self.v {
                    NumberMergerValue::Int(i2) => {
                        if i < i2 {
                            self.v = NumberMergerValue::Int(i);
                        }
                    }
                    NumberMergerValue::Float(f2) => {
                        let f = NotNan::new(i as f64).unwrap();
                        if f < f2 {
                            self.v = NumberMergerValue::Float(f);
                        }
                    }
                };
            }
            Value::Float(f) => {
                let f2 = match self.v {
                    NumberMergerValue::Int(i2) => NotNan::new(i2 as f64).unwrap(),
                    NumberMergerValue::Float(f2) => f2,
                };
                if f < f2 {
                    self.v = NumberMergerValue::Float(f);
                }
            }
            _ => {
                return Err(format!(
                    "expected numeric value, found: '{}'",
                    v.to_string_lossy()
                ));
            }
        }
        Ok(())
    }

    fn insert_into(
        self: Box<Self>,
        path: &OwnedTargetPath,
        v: &mut LogEvent,
    ) -> Result<(), String> {
        match self.v {
            NumberMergerValue::Float(f) => v.insert(path, Value::Float(f)),
            NumberMergerValue::Int(i) => v.insert(path, Value::Integer(i)),
        };
        Ok(())
    }
}

pub trait ReduceValueMerger: std::fmt::Debug + Send + Sync + DynClone {
    fn add(&mut self, v: Value) -> Result<(), String>;
    fn insert_into(self: Box<Self>, path: &OwnedTargetPath, v: &mut LogEvent)
        -> Result<(), String>;
}

dyn_clone::clone_trait_object!(ReduceValueMerger);

impl From<Value> for Box<dyn ReduceValueMerger> {
    fn from(v: Value) -> Self {
        match v {
            Value::Integer(i) => Box::new(AddNumbersMerger::new(i.into())),
            Value::Float(f) => Box::new(AddNumbersMerger::new(f.into())),
            Value::Timestamp(ts) => Box::new(TimestampWindowMerger::new(ts)),
            Value::Object(_) => Box::new(DiscardMerger::new(v)),
            Value::Null => Box::new(DiscardMerger::new(v)),
            Value::Boolean(_) => Box::new(DiscardMerger::new(v)),
            Value::Bytes(_) => Box::new(DiscardMerger::new(v)),
            Value::Regex(_) => Box::new(DiscardMerger::new(v)),
            Value::Array(_) => Box::new(DiscardMerger::new(v)),
        }
    }
}

pub(crate) fn get_value_merger(
    v: Value,
    m: &MergeStrategy,
) -> Result<Box<dyn ReduceValueMerger>, String> {
    match m {
        MergeStrategy::Sum => match v {
            Value::Integer(i) => Ok(Box::new(AddNumbersMerger::new(i.into()))),
            Value::Float(f) => Ok(Box::new(AddNumbersMerger::new(f.into()))),
            _ => Err(format!(
                "expected number value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::Max => match v {
            Value::Integer(i) => Ok(Box::new(MaxNumberMerger::new(i.into()))),
            Value::Float(f) => Ok(Box::new(MaxNumberMerger::new(f.into()))),
            _ => Err(format!(
                "expected number value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::Min => match v {
            Value::Integer(i) => Ok(Box::new(MinNumberMerger::new(i.into()))),
            Value::Float(f) => Ok(Box::new(MinNumberMerger::new(f.into()))),
            _ => Err(format!(
                "expected number value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::Concat => match v {
            Value::Bytes(b) => Ok(Box::new(ConcatMerger::new(b, Some(' ')))),
            Value::Array(a) => Ok(Box::new(ConcatArrayMerger::new(a))),
            _ => Err(format!(
                "expected string or array value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::ConcatNewline => match v {
            Value::Bytes(b) => Ok(Box::new(ConcatMerger::new(b, Some('\n')))),
            _ => Err(format!(
                "expected string value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::ConcatRaw => match v {
            Value::Bytes(b) => Ok(Box::new(ConcatMerger::new(b, None))),
            _ => Err(format!(
                "expected string value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::Array => Ok(Box::new(ArrayMerger::new(v))),
        MergeStrategy::ShortestArray => match v {
            Value::Array(a) => Ok(Box::new(ShortestArrayMerger::new(a))),
            _ => Err(format!(
                "expected array value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::LongestArray => match v {
            Value::Array(a) => Ok(Box::new(LongestArrayMerger::new(a))),
            _ => Err(format!(
                "expected array value, found: '{}'",
                v.to_string_lossy()
            )),
        },
        MergeStrategy::Discard => Ok(Box::new(DiscardMerger::new(v))),
        MergeStrategy::Retain => Ok(Box::new(RetainMerger::new(v))),
        MergeStrategy::FlatUnique => Ok(Box::new(FlatUniqueMerger::new(v))),
    }
}

