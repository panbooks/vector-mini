use std::{
    cmp, fmt,
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_stream::stream;
use crossbeam_queue::{ArrayQueue, SegQueue};
use futures::Stream;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::{config::MemoryBufferSize, InMemoryBufferable};

/// Error returned by `LimitedSender::send` when the receiver has disconnected.
#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "receiver disconnected")
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned by `LimitedSender::try_send`.
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    InsufficientCapacity(T),
    Disconnected(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::InsufficientCapacity(item) | Self::Disconnected(item) => item,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientCapacity(_) => {
                write!(fmt, "channel lacks sufficient capacity for send")
            }
            Self::Disconnected(_) => write!(fmt, "receiver disconnected"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}

// Trait over common queue operations so implementation can be chosen at initialization phase
trait QueueImpl<T>: Send + Sync + fmt::Debug {
    fn push(&self, item: T);
    fn pop(&self) -> Option<T>;
}

impl<T> QueueImpl<T> for ArrayQueue<T>
where
    T: Send + Sync + fmt::Debug,
{
    fn push(&self, item: T) {
        self.push(item)
            .unwrap_or_else(|_| unreachable!("acquired permits but channel reported being full."));
    }

    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

impl<T> QueueImpl<T> for SegQueue<T>
where
    T: Send + Sync + fmt::Debug,
{
    fn push(&self, item: T) {
        self.push(item);
    }

    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

#[derive(Debug)]
struct Inner<T> {
    data: Arc<dyn QueueImpl<(OwnedSemaphorePermit, T)>>,
    limit: MemoryBufferSize,
    limiter: Arc<Semaphore>,
    read_waker: Arc<Notify>,
}

impl<T> Clone for Inner<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            limit: self.limit,
            limiter: self.limiter.clone(),
            read_waker: self.read_waker.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LimitedSender<T> {
    inner: Inner<T>,
    sender_count: Arc<AtomicUsize>,
}

impl<T: InMemoryBufferable> LimitedSender<T> {
    #[allow(clippy::cast_possible_truncation)]
    fn get_required_permits_for_item(&self, item: &T) -> u32 {
        // We have to limit the number of permits we ask for to the overall limit since we're always
        // willing to store more items than the limit if the queue is entirely empty, because
        // otherwise we might deadlock ourselves by not being able to send a single item.
        let (limit, value) = match self.inner.limit {
            MemoryBufferSize::MaxSize(max_size) => (max_size, item.allocated_bytes()),
            MemoryBufferSize::MaxEvents(max_events) => (max_events, item.event_count()),
        };
        cmp::min(limit.get(), value) as u32
    }

    /// Gets the number of items that this channel could accept.
    pub fn available_capacity(&self) -> usize {
        self.inner.limiter.available_permits()
    }

    /// Sends an item into the channel.
    ///
    /// # Errors
    ///
    /// If the receiver has disconnected (does not exist anymore), then `Err(SendError)` be returned
    /// with the given `item`.
    pub async fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        // Calculate how many permits we need, and wait until we can acquire all of them.
        let permits_required = self.get_required_permits_for_item(&item);
        let Ok(permits) = self
            .inner
            .limiter
            .clone()
            .acquire_many_owned(permits_required)
            .await
        else {
            return Err(SendError(item));
        };

        self.inner.data.push((permits, item));
        self.inner.read_waker.notify_one();

        trace!("Sent item.");

        Ok(())
    }

    /// Attempts to send an item into the channel.
    ///
    /// # Errors
    ///
    /// If the receiver has disconnected (does not exist anymore), then
    /// `Err(TrySendError::Disconnected)` be returned with the given `item`. If the channel has
    /// insufficient capacity for the item, then `Err(TrySendError::InsufficientCapacity)` will be
    /// returned with the given `item`.
    ///
    /// # Panics
    ///
    /// Will panic if adding ack amount overflows.
    pub fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        // Calculate how many permits we need, and try to acquire them all without waiting.
        let permits_required = self.get_required_permits_for_item(&item);
        let permits = match self
            .inner
            .limiter
            .clone()
            .try_acquire_many_owned(permits_required)
        {
            Ok(permits) => permits,
            Err(ae) => {
                return match ae {
                    TryAcquireError::NoPermits => Err(TrySendError::InsufficientCapacity(item)),
                    TryAcquireError::Closed => Err(TrySendError::Disconnected(item)),
                }
            }
        };

        self.inner.data.push((permits, item));
        self.inner.read_waker.notify_one();

        trace!("Attempt to send item succeeded.");

        Ok(())
    }
}

impl<T> Clone for LimitedSender<T> {
    fn clone(&self) -> Self {
        self.sender_count.fetch_add(1, Ordering::SeqCst);

        Self {
            inner: self.inner.clone(),
            sender_count: Arc::clone(&self.sender_count),
        }
    }
}

impl<T> Drop for LimitedSender<T> {
    fn drop(&mut self) {
        // If we're the last sender to drop, close the semaphore on our way out the door.
        if self.sender_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.limiter.close();
            self.inner.read_waker.notify_one();
        }
    }
}

#[derive(Debug)]
pub struct LimitedReceiver<T> {
    inner: Inner<T>,
}

impl<T: Send + 'static> LimitedReceiver<T> {
    /// Gets the number of items that this channel could accept.
    pub fn available_capacity(&self) -> usize {
        self.inner.limiter.available_permits()
    }

    pub async fn next(&mut self) -> Option<T> {
        loop {
            if let Some((_permit, item)) = self.inner.data.pop() {
                return Some(item);
            }

            // There wasn't an item for us to pop, so see if the channel is actually closed.  If so,
            // then it's time for us to close up shop as well.
            if self.inner.limiter.is_closed() {
                return None;
            }

            // We're not closed, so we need to wait for a writer to tell us they made some
            // progress.  This might end up being a spurious wakeup since `Notify` will
            // store a wake-up if there are no waiters, but oh well.
            self.inner.read_waker.notified().await;
        }
    }

    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = T> + Send>> {
        let mut receiver = self;
        Box::pin(stream! {
            while let Some(item) = receiver.next().await {
                yield item;
            }
        })
    }
}

impl<T> Drop for LimitedReceiver<T> {
    fn drop(&mut self) {
        // Notify senders that the channel is now closed by closing the semaphore.  Any pending
        // acquisitions will be awoken and notified that the semaphore is closed, and further new
        // sends will immediately see the semaphore is closed.
        self.inner.limiter.close();
    }
}

pub fn limited<T: InMemoryBufferable + fmt::Debug>(
    limit: MemoryBufferSize,
) -> (LimitedSender<T>, LimitedReceiver<T>) {
    let inner = match limit {
        MemoryBufferSize::MaxEvents(max_events) => Inner {
            data: Arc::new(ArrayQueue::new(max_events.get())),
            limit,
            limiter: Arc::new(Semaphore::new(max_events.get())),
            read_waker: Arc::new(Notify::new()),
        },
        MemoryBufferSize::MaxSize(max_size) => Inner {
            data: Arc::new(SegQueue::new()),
            limit,
            limiter: Arc::new(Semaphore::new(max_size.get())),
            read_waker: Arc::new(Notify::new()),
        },
    };

    let sender = LimitedSender {
        inner: inner.clone(),
        sender_count: Arc::new(AtomicUsize::new(1)),
    };
    let receiver = LimitedReceiver { inner };

    (sender, receiver)
}

