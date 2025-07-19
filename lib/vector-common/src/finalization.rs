#![deny(missing_docs)]
//! This module contains the event metadata required to track an event
//! as it flows through transforms, being duplicated and merged, and
//! then report its status when the last copy is delivered or dropped.

use std::{cmp, future::Future, mem, pin::Pin, sync::Arc, task::Poll};

use crossbeam_utils::atomic::AtomicCell;
use futures::future::FutureExt;
use tokio::sync::oneshot;

#[cfg(feature = "byte_size_of")]
use crate::byte_size_of::ByteSizeOf;

/// A collection of event finalizers.
#[derive(Clone, Debug, Default)]
pub struct EventFinalizers(Vec<Arc<EventFinalizer>>);

impl Eq for EventFinalizers {}

impl PartialEq for EventFinalizers {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && (self.0.iter())
                .zip(other.0.iter())
                .all(|(a, b)| Arc::ptr_eq(a, b))
    }
}

impl PartialOrd for EventFinalizers {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        // There is no partial order defined structurally on
        // `EventFinalizer`. Partial equality is defined on the equality of
        // `Arc`s. Therefore, partial ordering of `EventFinalizers` is defined
        // only on the length of the finalizers.
        self.0.len().partial_cmp(&other.0.len())
    }
}

#[cfg(feature = "byte_size_of")]
impl ByteSizeOf for EventFinalizers {
    fn allocated_bytes(&self) -> usize {
        // Don't count the allocated data here, it's not really event
        // data we're interested in tracking but rather an artifact of
        // tracking and merging events.
        0
    }
}

impl EventFinalizers {
    /// Default empty finalizer set for use in `const` contexts.
    pub const DEFAULT: Self = Self(Vec::new());

    /// Creates a new `EventFinalizers` based on the given event finalizer.
    #[must_use]
    pub fn new(finalizer: EventFinalizer) -> Self {
        Self(vec![Arc::new(finalizer)])
    }

    /// Returns `true` if the collection contains no event finalizers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of event finalizers in the collection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Adds a new event finalizer to the collection.
    pub fn add(&mut self, finalizer: EventFinalizer) {
        self.0.push(Arc::new(finalizer));
    }

    /// Merges the event finalizers from `other` into the collection.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    /// Updates the status of all event finalizers in the collection.
    pub fn update_status(&self, status: EventStatus) {
        for finalizer in &self.0 {
            finalizer.update_status(status);
        }
    }

    /// Consumes all event finalizers and updates their underlying batches immediately.
    pub fn update_sources(&mut self) {
        let finalizers = mem::take(&mut self.0);
        for finalizer in &finalizers {
            finalizer.update_batch();
        }
    }
}

impl Finalizable for EventFinalizers {
    fn take_finalizers(&mut self) -> EventFinalizers {
        mem::take(self)
    }
}

impl std::iter::FromIterator<EventFinalizers> for EventFinalizers {
    fn from_iter<T: IntoIterator<Item = EventFinalizers>>(iter: T) -> Self {
        Self(iter.into_iter().flat_map(|f| f.0.into_iter()).collect())
    }
}

/// An event finalizer is the shared data required to handle tracking the status of an event, and updating the status of
/// a batch with that when the event is dropped.
#[derive(Debug)]
pub struct EventFinalizer {
    status: AtomicCell<EventStatus>,
    batch: BatchNotifier,
}

#[cfg(feature = "byte_size_of")]
impl ByteSizeOf for EventFinalizer {
    fn allocated_bytes(&self) -> usize {
        // Don't count the batch notifier, as it's shared across
        // events in a batch.
        0
    }
}

impl EventFinalizer {
    /// Creates a new `EventFinalizer` attached to the given `batch`.
    #[must_use]
    pub fn new(batch: BatchNotifier) -> Self {
        let status = AtomicCell::new(EventStatus::Dropped);
        Self { status, batch }
    }

    /// Updates the status of the event finalizer to `status`.
    pub fn update_status(&self, status: EventStatus) {
        self.status
            .fetch_update(|old_status| Some(old_status.update(status)))
            .unwrap_or_else(|_| unreachable!());
    }

    /// Updates the underlying batch status with the status of the event finalizer.
    ///
    /// In doing so, the event finalizer is marked as "recorded", which prevents any further updates to it.
    pub fn update_batch(&self) {
        let status = self
            .status
            .fetch_update(|_| Some(EventStatus::Recorded))
            .unwrap_or_else(|_| unreachable!());
        self.batch.update_status(status);
    }
}

impl Drop for EventFinalizer {
    fn drop(&mut self) {
        self.update_batch();
    }
}

/// A convenience newtype wrapper for the one-shot receiver for an
/// individual batch status.
#[pin_project::pin_project]
pub struct BatchStatusReceiver(oneshot::Receiver<BatchStatus>);

impl Future for BatchStatusReceiver {
    type Output = BatchStatus;
    fn poll(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.0.poll_unpin(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(status)) => Poll::Ready(status),
            Poll::Ready(Err(error)) => {
                error!(%error, "Batch status receiver dropped before sending.");
                Poll::Ready(BatchStatus::Errored)
            }
        }
    }
}

impl BatchStatusReceiver {
    /// Wrapper for the underlying `try_recv` function.
    ///
    /// # Errors
    ///
    /// - `TryRecvError::Empty` if no value has been sent yet.
    /// - `TryRecvError::Closed` if the sender has dropped without sending a value.
    pub fn try_recv(&mut self) -> Result<BatchStatus, oneshot::error::TryRecvError> {
        self.0.try_recv()
    }
}

/// A batch notifier contains the status of the current batch along with
/// a one-shot notifier to send that status back to the source. It is
/// shared among all events of a batch.
#[derive(Clone, Debug)]
pub struct BatchNotifier(Arc<OwnedBatchNotifier>);

impl BatchNotifier {
    /// Creates a new `BatchNotifier` along with the receiver used to await its finalization status.
    #[must_use]
    pub fn new_with_receiver() -> (Self, BatchStatusReceiver) {
        let (sender, receiver) = oneshot::channel();
        let notifier = OwnedBatchNotifier {
            status: AtomicCell::new(BatchStatus::Delivered),
            notifier: Some(sender),
        };
        (Self(Arc::new(notifier)), BatchStatusReceiver(receiver))
    }

    /// Optionally creates a new `BatchNotifier` along with the receiver used to await its finalization status.
    #[must_use]
    pub fn maybe_new_with_receiver(enabled: bool) -> (Option<Self>, Option<BatchStatusReceiver>) {
        if enabled {
            let (batch, receiver) = Self::new_with_receiver();
            (Some(batch), Some(receiver))
        } else {
            (None, None)
        }
    }

    /// Creates a new `BatchNotifier` and attaches it to a group of events.
    ///
    /// The receiver used to await the finalization status of the batch is returned.
    pub fn apply_to<T: AddBatchNotifier>(items: &mut [T]) -> BatchStatusReceiver {
        let (batch, receiver) = Self::new_with_receiver();
        for item in items {
            item.add_batch_notifier(batch.clone());
        }
        receiver
    }

    /// Optionally creates a new `BatchNotifier` and attaches it to a group of events.
    ///
    /// If `enabled`, the receiver used to await the finalization status of the batch is
    /// returned. Otherwise, `None` is returned.
    pub fn maybe_apply_to<T: AddBatchNotifier>(
        enabled: bool,
        items: &mut [T],
    ) -> Option<BatchStatusReceiver> {
        enabled.then(|| Self::apply_to(items))
    }

    /// Updates the status of the notifier.
    fn update_status(&self, status: EventStatus) {
        // The status starts as Delivered and can only change if the new
        // status is different than that.
        if status != EventStatus::Delivered && status != EventStatus::Dropped {
            self.0
                .status
                .fetch_update(|old_status| Some(old_status.update(status)))
                .unwrap_or_else(|_| unreachable!());
        }
    }
}

/// The non-shared data underlying the shared `BatchNotifier`
#[derive(Debug)]
pub struct OwnedBatchNotifier {
    status: AtomicCell<BatchStatus>,
    notifier: Option<oneshot::Sender<BatchStatus>>,
}

impl OwnedBatchNotifier {
    /// Sends the status of the notifier back to the source.
    fn send_status(&mut self) {
        if let Some(notifier) = self.notifier.take() {
            let status = self.status.load();
            // Ignore the error case, as it will happen during normal
            // source shutdown and we can't detect that here.
            _ = notifier.send(status);
        }
    }
}

impl Drop for OwnedBatchNotifier {
    fn drop(&mut self) {
        self.send_status();
    }
}

/// The status of an individual batch.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum BatchStatus {
    /// All events in the batch were accepted.
    ///
    /// This is the default.
    Delivered,
    /// At least one event in the batch had a transient error in delivery.
    Errored,
    /// At least one event in the batch had a permanent failure or rejection.
    Rejected,
}

impl Default for BatchStatus {
    fn default() -> Self {
        Self::Delivered
    }
}

impl BatchStatus {
    /// Updates the delivery status based on another batch's delivery status, returning the result.
    ///
    /// As not every status has the same priority, some updates may end up being a no-op either due to not being any
    /// different or due to being lower priority than the current status.
    #[allow(clippy::match_same_arms)] // False positive: https://github.com/rust-lang/rust-clippy/issues/860
    fn update(self, status: EventStatus) -> Self {
        match (self, status) {
            // `Dropped` and `Delivered` do not change the status.
            (_, EventStatus::Dropped | EventStatus::Delivered) => self,
            // `Rejected` overrides `Errored` and `Delivered`
            (Self::Rejected, _) | (_, EventStatus::Rejected) => Self::Rejected,
            // `Errored` overrides `Delivered`
            (Self::Errored, _) | (_, EventStatus::Errored) => Self::Errored,
            // No change for `Delivered`
            _ => self,
        }
    }
}

/// The status of an individual event.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum EventStatus {
    /// All copies of this event were dropped without being finalized.
    ///
    /// This is the default.
    Dropped,
    /// All copies of this event were delivered successfully.
    Delivered,
    /// At least one copy of this event encountered a retriable error.
    Errored,
    /// At least one copy of this event encountered a permanent failure or rejection.
    Rejected,
    /// This status has been recorded and should not be updated.
    Recorded,
}

impl Default for EventStatus {
    fn default() -> Self {
        Self::Dropped
    }
}

impl EventStatus {
    /// Updates the status based on another event's status, returning the result.
    ///
    /// As not every status has the same priority, some updates may end up being a no-op either due to not being any
    /// different or due to being lower priority than the current status.
    ///
    /// # Panics
    ///
    /// Passing a new status of `Dropped` is a programming error and will panic in debug/test builds.
    #[allow(clippy::match_same_arms)] // False positive: https://github.com/rust-lang/rust-clippy/issues/860
    #[must_use]
    pub fn update(self, status: Self) -> Self {
        match (self, status) {
            // `Recorded` always overwrites existing status and is never updated
            (_, Self::Recorded) | (Self::Recorded, _) => Self::Recorded,
            // `Dropped` always updates to the new status.
            (Self::Dropped, _) => status,
            // Updates *to* `Dropped` are nonsense.
            (_, Self::Dropped) => {
                debug_assert!(false, "Updating EventStatus to Dropped is nonsense");
                self
            }
            // `Rejected` overrides `Errored` or `Delivered`.
            (Self::Rejected, _) | (_, Self::Rejected) => Self::Rejected,
            // `Errored` overrides `Delivered`.
            (Self::Errored, _) | (_, Self::Errored) => Self::Errored,
            // No change for `Delivered`.
            (Self::Delivered, Self::Delivered) => Self::Delivered,
        }
    }
}

/// An object to which we can add a batch notifier.
pub trait AddBatchNotifier {
    /// Adds a single shared batch notifier to this type.
    fn add_batch_notifier(&mut self, notifier: BatchNotifier);
}

/// An object that can be finalized.
pub trait Finalizable {
    /// Consumes the finalizers of this object.
    ///
    /// Typically used for coalescing the finalizers of multiple items, such as when batching finalizable objects where
    /// all finalizations will be processed when the batch itself is processed.
    fn take_finalizers(&mut self) -> EventFinalizers;
}

impl<T: Finalizable> Finalizable for Vec<T> {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.iter_mut()
            .fold(EventFinalizers::default(), |mut acc, x| {
                acc.merge(x.take_finalizers());
                acc
            })
    }
}

