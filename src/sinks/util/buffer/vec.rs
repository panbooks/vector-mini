use bytes::Bytes;

use super::{err_event_too_large, Batch, BatchSize, PushResult};

pub trait EncodedLength {
    fn encoded_length(&self) -> usize;
}

/// Note: This has been deprecated, please do not use when creating new Sinks.
#[derive(Clone)]
pub struct VecBuffer<T> {
    batch: Option<Vec<T>>,
    bytes: usize,
    settings: BatchSize<Self>,
}

impl<T> VecBuffer<T> {
    pub const fn new(settings: BatchSize<Self>) -> Self {
        Self::new_with_settings(settings)
    }

    const fn new_with_settings(settings: BatchSize<Self>) -> Self {
        Self {
            batch: None,
            bytes: 0,
            settings,
        }
    }
}

impl<T: EncodedLength> Batch for VecBuffer<T> {
    type Input = T;
    type Output = Vec<T>;

    fn push(&mut self, item: Self::Input) -> PushResult<Self::Input> {
        let new_bytes = self.bytes + item.encoded_length();
        if self.is_empty() && item.encoded_length() > self.settings.bytes {
            err_event_too_large(item.encoded_length(), self.settings.bytes)
        } else if self.num_items() >= self.settings.events || new_bytes > self.settings.bytes {
            PushResult::Overflow(item)
        } else {
            let events = self.settings.events;
            let batch = self.batch.get_or_insert_with(|| Vec::with_capacity(events));
            batch.push(item);
            self.bytes = new_bytes;
            PushResult::Ok(batch.len() >= self.settings.events || new_bytes >= self.settings.bytes)
        }
    }

    fn is_empty(&self) -> bool {
        self.batch.as_ref().map(Vec::is_empty).unwrap_or(true)
    }

    fn fresh(&self) -> Self {
        Self::new_with_settings(self.settings)
    }

    fn finish(self) -> Self::Output {
        self.batch.unwrap_or_default()
    }

    fn num_items(&self) -> usize {
        self.batch.as_ref().map(Vec::len).unwrap_or(0)
    }
}

impl EncodedLength for Bytes {
    fn encoded_length(&self) -> usize {
        self.len()
    }
}

