use serde_json::value::{to_raw_value, RawValue, Value};

use super::super::batch::{err_event_too_large, Batch, BatchSize, PushResult};

pub type BoxedRawValue = Box<RawValue>;

/// A `batch` implementation for storing an array of json
/// values.
///
/// Note: This has been deprecated, please do not use when creating new Sinks.
#[derive(Debug)]
pub struct JsonArrayBuffer {
    buffer: Vec<BoxedRawValue>,
    total_bytes: usize,
    settings: BatchSize<Self>,
}

impl JsonArrayBuffer {
    pub const fn new(settings: BatchSize<Self>) -> Self {
        Self {
            buffer: Vec::new(),
            total_bytes: 0,
            settings,
        }
    }
}

impl Batch for JsonArrayBuffer {
    type Input = Value;
    type Output = Vec<BoxedRawValue>;

    fn push(&mut self, item: Self::Input) -> PushResult<Self::Input> {
        let raw_item = to_raw_value(&item).expect("Value should be valid json");
        let new_len = self.total_bytes + raw_item.get().len() + 1;
        if self.is_empty() && new_len >= self.settings.bytes {
            err_event_too_large(raw_item.get().len(), self.settings.bytes)
        } else if self.buffer.len() >= self.settings.events || new_len > self.settings.bytes {
            PushResult::Overflow(item)
        } else {
            self.total_bytes = new_len;
            self.buffer.push(raw_item);
            PushResult::Ok(
                self.buffer.len() >= self.settings.events || new_len >= self.settings.bytes,
            )
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn fresh(&self) -> Self {
        Self::new(self.settings)
    }

    fn finish(self) -> Self::Output {
        self.buffer
    }

    fn num_items(&self) -> usize {
        self.buffer.len()
    }
}

