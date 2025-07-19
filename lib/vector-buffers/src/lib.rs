//! The Vector Core buffer
//!
//! This library implements a channel like functionality, one variant which is
//! solely in-memory and the other that is on-disk. Both variants are bounded.

#![deny(warnings)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::type_complexity)] // long-types happen, especially in async code
#![allow(clippy::must_use_candidate)]
#![allow(async_fn_in_trait)]

#[macro_use]
extern crate tracing;

mod buffer_usage_data;

pub mod config;
pub use config::{BufferConfig, BufferType, MemoryBufferSize};
use encoding::Encodable;
use vector_config::configurable_component;

pub(crate) use vector_common::Result;

pub mod encoding;

mod internal_events;



/// An item that can be buffered in memory.
///
/// This supertrait serves as the base trait for any item that can be pushed into a memory buffer.
/// It is a relaxed version of `Bufferable` that allows for items that are not `Encodable` (e.g., `Instant`),
/// which is an unnecessary constraint for memory buffers.
pub trait InMemoryBufferable:
    AddBatchNotifier + ByteSizeOf + EventCount + Debug + Send + Sync + Unpin + Sized + 'static
{
}

// Blanket implementation for anything that is already in-memory bufferable.
impl<T> InMemoryBufferable for T where
    T: AddBatchNotifier + ByteSizeOf + EventCount + Debug + Send + Sync + Unpin + Sized + 'static
{
}

/// An item that can be buffered.
///
/// This supertrait serves as the base trait for any item that can be pushed into a buffer.
pub trait Bufferable: InMemoryBufferable + Encodable {}

// Blanket implementation for anything that is already bufferable.
impl<T> Bufferable for T where T: InMemoryBufferable + Encodable {}

pub trait EventCount {
    fn event_count(&self) -> usize;
}

impl<T> EventCount for Vec<T>
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        self.iter().map(EventCount::event_count).sum()
    }
}

impl<T> EventCount for &T
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        (*self).event_count()
    }
}

#[track_caller]
pub(crate) fn spawn_named<T>(
    task: impl std::future::Future<Output = T> + Send + 'static,
    _name: &str,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    return tokio::task::Builder::new()
        .name(_name)
        .spawn(task)
        .expect("tokio task should spawn");

    #[cfg(not(tokio_unstable))]
    tokio::spawn(task)
}
