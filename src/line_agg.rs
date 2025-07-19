//! A reusable line aggregation implementation.

#![deny(missing_docs)]

use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use regex::bytes::Regex;
use tokio_util::time::delay_queue::{DelayQueue, Key};
use vector_lib::configurable::configurable_component;

/// Mode of operation of the line aggregator.
#[configurable_component]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// All consecutive lines matching this pattern are included in the group.
    ///
    /// The first line (the line that matched the start pattern) does not need to match the `ContinueThrough` pattern.
    ///
    /// This is useful in cases such as a Java stack trace, where some indicator in the line (such as a leading
    /// whitespace) indicates that it is an extension of the proceeding line.
    ContinueThrough,

    /// All consecutive lines matching this pattern, plus one additional line, are included in the group.
    ///
    /// This is useful in cases where a log message ends with a continuation marker, such as a backslash, indicating
    /// that the following line is part of the same message.
    ContinuePast,

    /// All consecutive lines not matching this pattern are included in the group.
    ///
    /// This is useful where a log line contains a marker indicating that it begins a new message.
    HaltBefore,

    /// All consecutive lines, up to and including the first line matching this pattern, are included in the group.
    ///
    /// This is useful where a log line ends with a termination marker, such as a semicolon.
    HaltWith,
}

/// Configuration of multi-line aggregation.
#[derive(Clone, Debug)]
pub struct Config {
    /// Regular expression pattern that is used to match the start of a new message.
    pub start_pattern: Regex,

    /// Regular expression pattern that is used to determine whether or not more lines should be read.
    ///
    /// This setting must be configured in conjunction with `mode`.
    pub condition_pattern: Regex,

    /// Aggregation mode.
    ///
    /// This setting must be configured in conjunction with `condition_pattern`.
    pub mode: Mode,

    /// The maximum amount of time to wait for the next additional line, in milliseconds.
    ///
    /// Once this timeout is reached, the buffered message is guaranteed to be flushed, even if incomplete.
    pub timeout: Duration,
}

impl Config {
    /// Build `Config` from legacy `file` source line aggregator configuration
    /// params.
    pub fn for_legacy(marker: Regex, timeout_ms: u64) -> Self {
        let start_pattern = marker;
        let condition_pattern = start_pattern.clone();
        let mode = Mode::HaltBefore;
        let timeout = Duration::from_millis(timeout_ms);

        Self {
            start_pattern,
            condition_pattern,
            mode,
            timeout,
        }
    }
}

/// Line aggregator.
///
/// Provides a `Stream` implementation that reads lines from the `inner` stream
/// and yields aggregated lines.
#[pin_project(project = LineAggProj)]
pub struct LineAgg<T, K, C> {
    /// The stream from which we read the lines.
    #[pin]
    inner: T,

    /// The core line aggregation logic.
    logic: Logic<K, C>,

    /// Stashed lines. When line aggregation results in more than one line being
    /// emitted, we have to stash lines and return them into the stream after
    /// that before doing any other work.
    stashed: Option<(K, Bytes, C)>,

    /// Draining queue. We switch to draining mode when we get `None` from
    /// the inner stream. In this mode we stop polling `inner` for new lines
    /// and just flush all the buffered data.
    draining: Option<Vec<(K, Bytes, C, Option<C>)>>,
}

/// Core line aggregation logic.
///
/// Encapsulates the essential state and the core logic for the line
/// aggregation algorithm.
pub struct Logic<K, C> {
    /// Configuration parameters to use.
    config: Config,

    /// Line per key.
    /// Key is usually a filename or other line source identifier.
    buffers: HashMap<K, (Key, Aggregate<C>)>,

    /// A queue of key timeouts.
    timeouts: DelayQueue<K>,
}

impl<K, C> Logic<K, C> {
    /// Create a new `Logic` using the specified `Config`.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            buffers: HashMap::new(),
            timeouts: DelayQueue::new(),
        }
    }
}

impl<T, K, C> LineAgg<T, K, C>
where
    T: Stream<Item = (K, Bytes, C)> + Unpin,
    K: Hash + Eq + Clone,
{
    /// Create a new `LineAgg` using the specified `inner` stream and
    /// preconfigured `logic`.
    pub const fn new(inner: T, logic: Logic<K, C>) -> Self {
        Self {
            inner,
            logic,
            draining: None,
            stashed: None,
        }
    }
}

impl<T, K, C> Stream for LineAgg<T, K, C>
where
    T: Stream<Item = (K, Bytes, C)> + Unpin,
    K: Hash + Eq + Clone,
{
    /// `K` - file name, or other line source,
    /// `Bytes` - the line data,
    /// `C` - the initial context related to the first line of data.
    /// `Option<C>` - context related to the last-seen line data.
    type Item = (K, Bytes, C, Option<C>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            // If we have a stashed line, process it before doing anything else.
            if let Some((src, line, context)) = this.stashed.take() {
                // Handle the stashed line. If the handler gave us something -
                // return it, otherwise restart the loop iteration to start
                // anew. Handler could've stashed another value, continuing to
                // the new loop iteration handles that.
                if let Some(val) = Self::handle_line_and_stashing(&mut this, src, line, context) {
                    return Poll::Ready(Some(val));
                }
                continue;
            }

            // If we're in draining mode, short circuit here.
            if let Some(to_drain) = &mut this.draining {
                match to_drain.pop() {
                    Some(val) => {
                        return Poll::Ready(Some(val));
                    }
                    _ => {
                        return Poll::Ready(None);
                    }
                }
            }

            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some((src, line, context))) => {
                    // Handle the incoming line we got from `inner`. If the
                    // handler gave us something - return it, otherwise continue
                    // with the flow.
                    if let Some(val) = Self::handle_line_and_stashing(&mut this, src, line, context)
                    {
                        return Poll::Ready(Some(val));
                    }
                }
                Poll::Ready(None) => {
                    // We got `None`, this means the `inner` stream has ended.
                    // Start flushing all existing data, stop polling `inner`.
                    *this.draining = Some(
                        this.logic
                            .buffers
                            .drain()
                            .map(|(src, (_, aggregate))| {
                                let (line, initial_context, last_context) = aggregate.merge();
                                (src, line, initial_context, last_context)
                            })
                            .collect(),
                    );
                }
                Poll::Pending => {
                    // We didn't get any lines from `inner`, so we just give
                    // a line from keys that have hit their timeout.
                    while let Poll::Ready(Some(expired_key)) = this.logic.timeouts.poll_expired(cx)
                    {
                        let key = expired_key.into_inner();
                        if let Some((_, aggregate)) = this.logic.buffers.remove(&key) {
                            let (line, initial_context, last_context) = aggregate.merge();
                            return Poll::Ready(Some((key, line, initial_context, last_context)));
                        }
                    }

                    return Poll::Pending;
                }
            };
        }
    }
}

impl<T, K, C> LineAgg<T, K, C>
where
    T: Stream<Item = (K, Bytes, C)> + Unpin,
    K: Hash + Eq + Clone,
{
    /// Handle line and do stashing of extra emitted lines.
    /// Requires that the `stashed` item is empty (i.e. entry is vacant). This
    /// invariant has to be taken care of by the caller.
    fn handle_line_and_stashing(
        this: &mut LineAggProj<'_, T, K, C>,
        src: K,
        line: Bytes,
        context: C,
    ) -> Option<(K, Bytes, C, Option<C>)> {
        // Stashed line is always consumed at the start of the `poll`
        // loop before entering this line processing logic. If it's
        // non-empty here - it's a bug.
        debug_assert!(this.stashed.is_none());
        let val = this.logic.handle_line(src, line, context)?;
        let val = match val {
            // If we have to emit just one line - that's easy,
            // we just return it.
            (src, Emit::One((line, initial_context, last_context))) => {
                (src, line, initial_context, last_context)
            }
            // If we have to emit two lines - take the second
            // one and stash it, then return the first one.
            // This way, the stashed line will be returned
            // on the next stream poll.
            (
                src,
                Emit::Two(
                    (line, initial_context, last_context),
                    (line_to_stash, context_to_stash, _),
                ),
            ) => {
                *this.stashed = Some((src.clone(), line_to_stash, context_to_stash));
                (src, line, initial_context, last_context)
            }
        };
        Some(val)
    }
}

/// Specifies the amount of lines to emit in response to a single input line.
/// We have to emit either one or two lines.
pub enum Emit<T> {
    /// Emit one line.
    One(T),
    /// Emit two lines, in the order they're specified.
    Two(T, T),
}

/// A helper enum
enum Decision {
    Continue,
    EndInclude,
    EndExclude,
}

impl<K, C> Logic<K, C>
where
    K: Hash + Eq + Clone,
{
    /// Handle line, if we have something to output - return it.
    pub fn handle_line(
        &mut self,
        src: K,
        line: Bytes,
        context: C,
    ) -> Option<(K, Emit<(Bytes, C, Option<C>)>)> {
        // Check if we already have the buffered data for the source.
        match self.buffers.entry(src) {
            Entry::Occupied(mut entry) => {
                let condition_matched = self.config.condition_pattern.is_match(line.as_ref());
                let decision = match (self.config.mode, condition_matched) {
                    // All consecutive lines matching this pattern are included in
                    // the group.
                    (Mode::ContinueThrough, true) => Decision::Continue,
                    (Mode::ContinueThrough, false) => Decision::EndExclude,
                    // All consecutive lines matching this pattern, plus one
                    // additional line, are included in the group.
                    (Mode::ContinuePast, true) => Decision::Continue,
                    (Mode::ContinuePast, false) => Decision::EndInclude,
                    // All consecutive lines not matching this pattern are included
                    // in the group.
                    (Mode::HaltBefore, true) => Decision::EndExclude,
                    (Mode::HaltBefore, false) => Decision::Continue,
                    // All consecutive lines, up to and including the first line
                    // matching this pattern, are included in the group.
                    (Mode::HaltWith, true) => Decision::EndInclude,
                    (Mode::HaltWith, false) => Decision::Continue,
                };

                match decision {
                    Decision::Continue => {
                        let buffered = entry.get_mut();
                        self.timeouts.reset(&buffered.0, self.config.timeout);
                        buffered.1.add_next_line(line, context);
                        None
                    }
                    Decision::EndInclude => {
                        let (src, (key, mut buffered)) = entry.remove_entry();
                        self.timeouts.remove(&key);
                        buffered.add_next_line(line, context);
                        Some((src, Emit::One(buffered.merge())))
                    }
                    Decision::EndExclude => {
                        let (src, (key, buffered)) = entry.remove_entry();
                        self.timeouts.remove(&key);
                        Some((src, Emit::Two(buffered.merge(), (line, context, None))))
                    }
                }
            }
            Entry::Vacant(entry) => {
                // This line is a candidate for buffering, or passing through.
                if self.config.start_pattern.is_match(line.as_ref()) {
                    // It was indeed a new line we need to filter.
                    // Set the timeout and buffer this line.
                    let key = self
                        .timeouts
                        .insert(entry.key().clone(), self.config.timeout);
                    entry.insert((key, Aggregate::new(line, context)));
                    None
                } else {
                    // It's just a regular line we don't really care about.
                    Some((entry.into_key(), Emit::One((line, context, None))))
                }
            }
        }
    }
}

struct Aggregate<C> {
    lines: Vec<Bytes>,
    initial_context: C,
    last_context: Option<C>,
}

impl<C> Aggregate<C> {
    fn new(first_line: Bytes, initial_context: C) -> Self {
        Self {
            lines: vec![first_line],
            initial_context,
            last_context: None,
        }
    }

    fn add_next_line(&mut self, line: Bytes, context: C) {
        self.last_context = Some(context);
        self.lines.push(line);
    }

    fn merge(self) -> (Bytes, C, Option<C>) {
        let capacity = self.lines.iter().map(|line| line.len() + 1).sum::<usize>() - 1;
        let mut bytes_mut = BytesMut::with_capacity(capacity);
        let mut first = true;
        for line in self.lines {
            if first {
                first = false;
            } else {
                bytes_mut.extend_from_slice(b"\n");
            }
            bytes_mut.extend_from_slice(&line);
        }
        (bytes_mut.freeze(), self.initial_context, self.last_context)
    }
}

