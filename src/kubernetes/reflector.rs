//! Intercept [`watcher::Event`]'s.

use std::{hash::Hash, sync::Arc, time::Duration};

use futures::StreamExt;
use futures_util::Stream;
use kube::{
    runtime::{reflector::store, watcher},
    Resource,
};
use tokio::pin;
use tokio_util::time::DelayQueue;

use super::meta_cache::{MetaCache, MetaDescribe};

/// Handles events from a [`kube::runtime::watcher()`] to delay the application of Deletion events.
pub async fn custom_reflector<K, W>(
    mut store: store::Writer<K>,
    mut meta_cache: MetaCache,
    stream: W,
    delay_deletion: Duration,
) where
    K: Resource + Clone + std::fmt::Debug,
    K::DynamicType: Eq + Hash + Clone,
    W: Stream<Item = watcher::Result<watcher::Event<K>>>,
{
    pin!(stream);
    let mut delay_queue = DelayQueue::default();
    let mut init_buffer_meta = Vec::new();
    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(event)) => {
                        match event {
                            // Immediately reconcile `Apply` event
                            watcher::Event::Apply(ref obj) => {
                                trace!(message = "Processing Apply event.", event_type = std::any::type_name::<K>(), event = ?event);
                                store.apply_watcher_event(&event);
                                let meta_descr = MetaDescribe::from_meta(obj.meta());
                                meta_cache.store(meta_descr);
                            }
                            // Delay reconciling any `Delete` events
                            watcher::Event::Delete(ref obj) => {
                                trace!(message = "Delaying processing Delete event.", event_type = std::any::type_name::<K>(), event = ?event);
                                delay_queue.insert(event.to_owned(), delay_deletion);
                                let meta_descr = MetaDescribe::from_meta(obj.meta());
                                meta_cache.delete(&meta_descr);
                            }
                            // Clear all delayed events on `Init` event
                            watcher::Event::Init => {
                                trace!(message = "Processing Init event.", event_type = std::any::type_name::<K>(), event = ?event);
                                delay_queue.clear();
                                store.apply_watcher_event(&event);
                                meta_cache.clear();
                                init_buffer_meta.clear();
                            }
                            // Immediately reconcile `InitApply` event (but buffer the obj ref so we can handle implied deletions on InitDone)
                            watcher::Event::InitApply(ref obj) => {
                                trace!(message = "Processing InitApply event.", event_type = std::any::type_name::<K>(), event = ?event);
                                store.apply_watcher_event(&event);
                                let meta_descr = MetaDescribe::from_meta(obj.meta());
                                meta_cache.store(meta_descr.clone());
                                init_buffer_meta.push(meta_descr.clone());
                            }
                            // Reconcile `InitApply` events and implied deletions
                            watcher::Event::InitDone => {
                                trace!(message = "Processing InitDone event.", event_type = std::any::type_name::<K>(), event = ?event);
                                store.apply_watcher_event(&event);


                                store.as_reader().state().into_iter()
                                // delay deleting objs that were added before but not during InitApply
                                .for_each(|obj| {
                                    if let Some(inner) = Arc::into_inner(obj) {
                                        let meta_descr = MetaDescribe::from_meta(inner.meta());
                                        if !init_buffer_meta.contains(&meta_descr) {
                                            let implied_deletion_event = watcher::Event::Delete(inner);
                                            trace!(message = "Delaying processing implied deletion.", event_type = std::any::type_name::<K>(), event = ?implied_deletion_event);
                                            delay_queue.insert(implied_deletion_event, delay_deletion);
                                            meta_cache.delete(&meta_descr);
                                        }
                                    }
                                });

                                init_buffer_meta.clear();
                            }
                        }
                    },
                    Some(Err(error)) => {
                        warn!(message = "Watcher Stream received an error. Retrying.", ?error);
                    },
                    // The watcher stream should never yield `None`
                    // https://docs.rs/kube-runtime/0.71.0/src/kube_runtime/watcher.rs.html#234-237
                    None => {
                        unreachable!("a watcher Stream never ends");
                    },
                }
            }
            result = delay_queue.next(), if !delay_queue.is_empty() => {
                match result {
                    Some(event) => {
                        let event = event.into_inner();
                        match event {
                            watcher::Event::Delete(ref obj) => {
                                let meta_desc = MetaDescribe::from_meta(obj.meta());
                                if !meta_cache.contains(&meta_desc) {
                                    trace!(message = "Processing Delete event.", event_type = std::any::type_name::<K>(), event = ?event);
                                    store.apply_watcher_event(&event);
                                }
                            },
                            _ => store.apply_watcher_event(&event),
                        }
                    },
                    // DelayQueue returns None if the queue is exhausted,
                    // however we disable the DelayQueue branch if there are
                    // no items in the queue.
                    None => {
                        unreachable!("an empty DelayQueue is never polled");
                    },
                }
            }
        }
    }
}

