use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hash},
    num::NonZeroUsize,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use futures::stream::{Fuse, Stream, StreamExt};
use pin_project::pin_project;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use twox_hash::XxHash64;
use vector_common::byte_size_of::ByteSizeOf;
use vector_core::{partition::Partitioner, time::KeyedTimer};

use crate::batcher::{
    config::BatchConfigParts,
    data::BatchData,
    limiter::{ByteSizeOfItemSize, ItemBatchSize, SizeLimit},
    BatchConfig,
};

/// A `KeyedTimer` based on `DelayQueue`.
pub struct ExpirationQueue<K> {
    /// The timeout to give each new key entry
    timeout: Duration,
    /// The queue of expirations
    expirations: DelayQueue<K>,
    /// The Key -> K mapping, allows for resets
    expiration_map: HashMap<K, Key>,
}

impl<K> ExpirationQueue<K> {
    /// Creates a new `ExpirationQueue`.
    ///
    /// `timeout` is used for all insertions and resets.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            expirations: DelayQueue::new(),
            expiration_map: HashMap::default(),
        }
    }

    /// The number of current subtimers in the queue.
    ///
    /// Includes subtimers which have expired but have not yet been removed via
    /// calls to `poll_expired`.
    pub fn len(&self) -> usize {
        self.expirations.len()
    }

    /// Returns `true` if the queue has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K> KeyedTimer<K> for ExpirationQueue<K>
where
    K: Eq + Hash + Clone,
{
    fn clear(&mut self) {
        self.expirations.clear();
        self.expiration_map.clear();
    }

    fn insert(&mut self, item_key: K) {
        if let Some(expiration_key) = self.expiration_map.get(&item_key) {
            // We already have an expiration entry for this item key, so
            // just reset the expiration.
            self.expirations.reset(expiration_key, self.timeout);
        } else {
            // This is a yet-unseen item key, so create a new expiration
            // entry.
            let expiration_key = self.expirations.insert(item_key.clone(), self.timeout);
            assert!(self
                .expiration_map
                .insert(item_key, expiration_key)
                .is_none());
        }
    }

    fn remove(&mut self, item_key: &K) {
        if let Some(expiration_key) = self.expiration_map.remove(item_key) {
            self.expirations.remove(&expiration_key);
        }
    }

    fn poll_expired(&mut self, cx: &mut Context) -> Poll<Option<K>> {
        match ready!(self.expirations.poll_expired(cx)) {
            // No expirations yet.
            None => Poll::Ready(None),
            Some(expiration) => {
                // An item has expired, so remove it from the map and return
                // it.
                assert!(self.expiration_map.remove(expiration.get_ref()).is_some());
                Poll::Ready(Some(expiration.into_inner()))
            }
        }
    }
}

/// Controls the behavior of the batcher in terms of batch size and flush interval.
///
/// This is a temporary solution for pushing in a fixed settings structure so we don't have to worry
/// about misordering parameters and what not.  At some point, we will pull
/// `BatchConfig`/`BatchSettings`/`BatchSize` out of `vector` and move them into `vector_core`, and
/// make it more generalized. We can't do that yet, though, until we've converted all of the sinks
/// with their various specialized batch buffers.
#[derive(Copy, Clone, Debug)]
pub struct BatcherSettings {
    pub timeout: Duration,
    pub size_limit: usize,
    pub item_limit: usize,
}

impl BatcherSettings {
    pub const fn new(
        timeout: Duration,
        size_limit: NonZeroUsize,
        item_limit: NonZeroUsize,
    ) -> Self {
        BatcherSettings {
            timeout,
            size_limit: size_limit.get(),
            item_limit: item_limit.get(),
        }
    }

    /// A batcher config using the `ByteSizeOf` trait to determine batch sizes.
    /// The output is a  `Vec<T>`.
    pub fn as_byte_size_config<T: ByteSizeOf>(
        &self,
    ) -> BatchConfigParts<SizeLimit<ByteSizeOfItemSize>, Vec<T>> {
        self.as_item_size_config(ByteSizeOfItemSize)
    }

    /// A batcher config using the `ItemBatchSize` trait to determine batch sizes.
    /// The output is a `Vec<T>`.
    pub fn as_item_size_config<T, I>(&self, item_size: I) -> BatchConfigParts<SizeLimit<I>, Vec<T>>
    where
        I: ItemBatchSize<T>,
    {
        BatchConfigParts {
            batch_limiter: SizeLimit {
                batch_size_limit: self.size_limit,
                batch_item_limit: self.item_limit,
                current_size: 0,
                item_size_calculator: item_size,
            },
            batch_data: vec![],
            timeout: self.timeout,
        }
    }

    /// A batcher config using the `ItemBatchSize` trait to determine batch sizes.
    /// The output is built with the supplied object implementing [`BatchData`].
    pub fn as_reducer_config<I, T, B>(
        &self,
        item_size: I,
        reducer: B,
    ) -> BatchConfigParts<SizeLimit<I>, B>
    where
        I: ItemBatchSize<T>,
        B: BatchData<T>,
    {
        BatchConfigParts {
            batch_limiter: SizeLimit {
                batch_size_limit: self.size_limit,
                batch_item_limit: self.item_limit,
                current_size: 0,
                item_size_calculator: item_size,
            },
            batch_data: reducer,
            timeout: self.timeout,
        }
    }
}

#[pin_project]
pub struct PartitionedBatcher<St, Prt, KT, C, F, B>
where
    Prt: Partitioner,
{
    /// A closure that retrieves a new [`BatchConfig`] when needed to batch a
    /// new partition.
    state: F,
    /// The store of live batches. Note that the key here is an option type,
    /// on account of the interface of `Prt`.
    batches: HashMap<Prt::Key, C, BuildHasherDefault<XxHash64>>,
    /// The store of 'closed' batches. When this is not empty it will be
    /// preferentially flushed prior to consuming any new items from the
    /// underlying stream.
    closed_batches: Vec<(Prt::Key, B)>,
    /// The queue of pending batch expirations
    timer: KT,
    /// The partitioner for this `Batcher`
    partitioner: Prt,
    #[pin]
    /// The stream this `Batcher` wraps
    stream: Fuse<St>,
}

impl<St, Prt, C, F, B> PartitionedBatcher<St, Prt, ExpirationQueue<Prt::Key>, C, F, B>
where
    St: Stream<Item = Prt::Item>,
    Prt: Partitioner + Unpin,
    Prt::Key: Eq + Hash + Clone,
    Prt::Item: ByteSizeOf,
    C: BatchConfig<Prt::Item>,
    F: Fn() -> C + Send,
{
    pub fn new(stream: St, partitioner: Prt, settings: F) -> Self {
        let timeout = settings().timeout();
        Self {
            state: settings,
            batches: HashMap::default(),
            closed_batches: Vec::default(),
            timer: ExpirationQueue::new(timeout),
            partitioner,
            stream: stream.fuse(),
        }
    }
}


impl<St, Prt, KT, C, F, B> Stream for PartitionedBatcher<St, Prt, KT, C, F, B>
where
    St: Stream<Item = Prt::Item>,
    Prt: Partitioner + Unpin,
    Prt::Key: Eq + Hash + Clone,
    Prt::Item: ByteSizeOf,
    KT: KeyedTimer<Prt::Key>,
    C: BatchConfig<Prt::Item, Batch = B>,
    F: Fn() -> C + Send,
{
    type Item = (Prt::Key, B);

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if !this.closed_batches.is_empty() {
                return Poll::Ready(this.closed_batches.pop());
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => match this.timer.poll_expired(cx) {
                    // Unlike normal streams, `DelayQueue` can return `None`
                    // here but still be usable later if more entries are added.
                    Poll::Pending | Poll::Ready(None) => return Poll::Pending,
                    Poll::Ready(Some(item_key)) => {
                        let mut batch = this
                            .batches
                            .remove(&item_key)
                            .expect("batch should exist if it is set to expire");
                        this.closed_batches.push((item_key, batch.take_batch()));
                    }
                },
                Poll::Ready(None) => {
                    // Now that the underlying stream is closed, we need to
                    // clear out our batches, including all expiration
                    // entries. If we had any batches to hand over, we have to
                    // continue looping so the caller can drain them all before
                    // we finish.
                    if !this.batches.is_empty() {
                        this.timer.clear();
                        this.closed_batches.extend(
                            this.batches
                                .drain()
                                .map(|(key, mut batch)| (key, batch.take_batch())),
                        );
                        continue;
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(item)) => {
                    let item_key = this.partitioner.partition(&item);

                    // Get the batch for this partition, or create a new one.
                    let batch = if let Some(batch) = this.batches.get_mut(&item_key) {
                        batch
                    } else {
                        let batch = (this.state)();
                        this.batches.insert(item_key.clone(), batch);
                        this.timer.insert(item_key.clone());
                        this.batches
                            .get_mut(&item_key)
                            .expect("batch has just been inserted so should exist")
                    };

                    let (fits, metadata) = batch.item_fits_in_batch(&item);
                    if !fits {
                        // This batch is too full to accept a new item, so we move the contents of
                        // the batch into `closed_batches` to be push out of this stream on the
                        // next iteration.
                        this.closed_batches
                            .push((item_key.clone(), batch.take_batch()));

                        // The batch for this partition key was set to
                        // expire, but now it's overflowed and must be
                        // pushed out, so now we reset the batch timeout.
                        this.timer.insert(item_key.clone());
                    }

                    // Insert the item into the batch.
                    batch.push(item, metadata);
                    if batch.is_batch_full() {
                        // If the insertion means the batch is now full, we clear out the batch and
                        // remove it from the list.
                        this.closed_batches
                            .push((item_key.clone(), batch.take_batch()));
                        this.batches.remove(&item_key);
                        this.timer.remove(&item_key);
                    }
                }
            }
        }
    }
}

#[allow(clippy::cast_sign_loss)]
