pub mod config;
pub mod data;
pub mod limiter;

use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

pub use config::BatchConfig;
use futures::{
    stream::{Fuse, Stream},
    Future, StreamExt,
};
use pin_project::pin_project;
use tokio::time::Sleep;

#[pin_project]
pub struct Batcher<S, C> {
    state: C,

    #[pin]
    /// The stream this `Batcher` wraps
    stream: Fuse<S>,

    #[pin]
    timer: Maybe<Sleep>,
}

/// An `Option`, but with pin projection
#[pin_project(project = MaybeProj)]
pub enum Maybe<T> {
    Some(#[pin] T),
    None,
}

impl<S, C> Batcher<S, C>
where
    S: Stream,
    C: BatchConfig<S::Item>,
{
    pub fn new(stream: S, config: C) -> Self {
        Self {
            state: config,
            stream: stream.fuse(),
            timer: Maybe::None,
        }
    }
}

impl<S, C> Stream for Batcher<S, C>
where
    S: Stream,
    C: BatchConfig<S::Item>,
{
    type Item = C::Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            match this.stream.poll_next(cx) {
                Poll::Ready(None) => {
                    return {
                        if this.state.len() == 0 {
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(this.state.take_batch()))
                        }
                    }
                }
                Poll::Ready(Some(item)) => {
                    let (item_fits, item_metadata) = this.state.item_fits_in_batch(&item);
                    if item_fits {
                        this.state.push(item, item_metadata);
                        if this.state.is_batch_full() {
                            this.timer.set(Maybe::None);
                            return Poll::Ready(Some(this.state.take_batch()));
                        } else if this.state.len() == 1 {
                            this.timer
                                .set(Maybe::Some(tokio::time::sleep(this.state.timeout())));
                        }
                    } else {
                        let output = Poll::Ready(Some(this.state.take_batch()));
                        this.state.push(item, item_metadata);
                        this.timer
                            .set(Maybe::Some(tokio::time::sleep(this.state.timeout())));
                        return output;
                    }
                }
                Poll::Pending => {
                    return {
                        if let MaybeProj::Some(timer) = this.timer.as_mut().project() {
                            ready!(timer.poll(cx));
                            this.timer.set(Maybe::None);
                            debug_assert!(
                                this.state.len() != 0,
                                "timer should have been cancelled"
                            );
                            Poll::Ready(Some(this.state.take_batch()))
                        } else {
                            Poll::Pending
                        }
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

