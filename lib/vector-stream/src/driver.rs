use std::{collections::VecDeque, fmt, future::poll_fn, task::Poll};

use futures::{poll, FutureExt, Stream, StreamExt, TryFutureExt};
use tokio::{pin, select};
use tower::Service;
use tracing::Instrument;
use vector_common::internal_event::emit;
use vector_common::internal_event::{
    register, ByteSize, BytesSent, CallError, InternalEventHandle as _, PollReadyError, Registered,
    RegisteredEventCache, SharedString, TaggedEventsSent,
};
use vector_common::request_metadata::{GroupedCountByteSize, MetaDescriptive};
use vector_core::event::{EventFinalizers, EventStatus, Finalizable};

use super::FuturesUnorderedCount;

pub trait DriverResponse {
    fn event_status(&self) -> EventStatus;
    fn events_sent(&self) -> &GroupedCountByteSize;

    /// Return the number of bytes that were sent in the request that returned this response.
    // TODO, remove the default implementation once all sinks have
    // implemented this function.
    fn bytes_sent(&self) -> Option<usize> {
        None
    }
}

/// Drives the interaction between a stream of items and a service which processes them
/// asynchronously.
///
/// `Driver`, as a high-level, facilitates taking items from an arbitrary `Stream` and pushing them
/// through a `Service`, spawning each call to the service so that work can be run concurrently,
/// managing waiting for the service to be ready before processing more items, and so on.
///
/// Additionally, `Driver` handles event finalization, which triggers acknowledgements
/// to the source or disk buffer.
///
/// This capability is parameterized so any implementation which can define how to interpret the
/// response for each request, as well as define how many events a request is compromised of, can be
/// used with `Driver`.
pub struct Driver<St, Svc> {
    input: St,
    service: Svc,
    protocol: Option<SharedString>,
}

impl<St, Svc> Driver<St, Svc> {
    pub fn new(input: St, service: Svc) -> Self {
        Self {
            input,
            service,
            protocol: None,
        }
    }

    /// Set the protocol name for this driver.
    ///
    /// If this is set, the driver will fetch and use the `bytes_sent` value from responses in a
    /// `BytesSent` event.
    #[must_use]
    pub fn protocol(mut self, protocol: impl Into<SharedString>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }
}

impl<St, Svc> Driver<St, Svc>
where
    St: Stream,
    St::Item: Finalizable + MetaDescriptive,
    Svc: Service<St::Item>,
    Svc::Error: fmt::Debug + 'static,
    Svc::Future: Send + 'static,
    Svc::Response: DriverResponse,
{
    /// Runs the driver until the input stream is exhausted.
    ///
    /// All in-flight calls to the provided `service` will also be completed before `run` returns.
    ///
    /// # Errors
    ///
    /// The return type is mostly to simplify caller code.
    /// An error is currently only returned if a service returns an error from `poll_ready`
    pub async fn run(self) -> Result<(), ()> {
        let mut in_flight = FuturesUnorderedCount::new();
        let mut next_batch: Option<VecDeque<St::Item>> = None;
        let mut seq_num = 0usize;

        let Self {
            input,
            mut service,
            protocol,
        } = self;

        let batched_input = input.ready_chunks(1024);
        pin!(batched_input);

        let bytes_sent = protocol.map(|protocol| register(BytesSent { protocol }));
        let events_sent = RegisteredEventCache::new(());

        loop {
            // Core behavior of the loop:
            // - always check to see if we have any response futures that have completed
            //  -- if so, handling acking as many events as we can (ordering matters)
            // - if we have a "current" batch, try to send each request in it to the service
            //   -- if we can't drain all requests from the batch due to lack of service readiness,
            //   then put the batch back and try to send the rest of it when the service is ready
            //   again
            // - if we have no "current" batch, but there is an available batch from our input
            //   stream, grab that batch and store it as our current batch
            //
            // Essentially, we bounce back and forth between "grab the new batch from the input
            // stream" and "send all requests in the batch to our service" which _could be trivially
            // modeled with a normal imperative loop.  However, we want to be able to interleave the
            // acknowledgement of responses to allow buffers and sources to continue making forward
            // progress, which necessitates a more complex weaving of logic.  Using `select!` is
            // more code, and requires a more careful eye than blindly doing
            // "get_next_batch().await; process_batch().await", but it does make doing the complex
            // logic easier than if we tried to interleave it ourselves with an imperative-style loop.

            select! {
                // Using `biased` ensures we check the branches in the order they're written, since
                // the default behavior of the `select!` macro is to randomly order branches as a
                // means of ensuring scheduling fairness.
                biased;

                // One or more of our service calls have completed.
                Some(_count) = in_flight.next(), if !in_flight.is_empty() => {}

                // We've got an input batch to process and the service is ready to accept a request.
                maybe_ready = poll_fn(|cx| service.poll_ready(cx)), if next_batch.is_some() => {
                    let mut batch = next_batch.take()
                        .unwrap_or_else(|| unreachable!("batch should be populated"));

                    let mut maybe_ready = Some(maybe_ready);
                    while !batch.is_empty() {
                        // Make sure the service is ready to take another request.
                        let maybe_ready = match maybe_ready.take() {
                            Some(ready) => Poll::Ready(ready),
                            None => poll!(poll_fn(|cx| service.poll_ready(cx))),
                        };

                        let svc = match maybe_ready {
                            Poll::Ready(Ok(())) => &mut service,
                            Poll::Ready(Err(error)) => {
                                emit(PollReadyError{ error });
                                return Err(())
                            }
                            Poll::Pending => {
                                next_batch = Some(batch);
                                break
                            },
                        };

                        let mut req = batch.pop_front().unwrap_or_else(|| unreachable!("batch should not be empty"));
                        seq_num += 1;
                        let request_id = seq_num;

                        trace!(
                            message = "Submitting service request.",
                            in_flight_requests = in_flight.len(),
                            request_id,
                        );
                        let finalizers = req.take_finalizers();
                        let bytes_sent = bytes_sent.clone();
                        let events_sent = events_sent.clone();
                        let event_count = req.get_metadata().event_count();

                        let fut = svc.call(req)
                            .err_into()
                            .map(move |result| Self::handle_response(
                                result,
                                request_id,
                                finalizers,
                                event_count,
                                bytes_sent.as_ref(),
                                &events_sent,
                            ))
                            .instrument(info_span!("request", request_id).or_current());

                        in_flight.push(fut);
                    }
                }

                // We've received some items from the input stream.
                Some(reqs) = batched_input.next(), if next_batch.is_none() => {
                    next_batch = Some(reqs.into());
                }

                else => break
            }
        }

        Ok(())
    }

    fn handle_response(
        result: Result<Svc::Response, Svc::Error>,
        request_id: usize,
        finalizers: EventFinalizers,
        event_count: usize,
        bytes_sent: Option<&Registered<BytesSent>>,
        events_sent: &RegisteredEventCache<(), TaggedEventsSent>,
    ) {
        match result {
            Err(error) => {
                Self::emit_call_error(Some(error), request_id, event_count);
                finalizers.update_status(EventStatus::Rejected);
            }
            Ok(response) => {
                trace!(message = "Service call succeeded.", request_id);
                finalizers.update_status(response.event_status());
                if response.event_status() == EventStatus::Delivered {
                    if let Some(bytes_sent) = bytes_sent {
                        if let Some(byte_size) = response.bytes_sent() {
                            bytes_sent.emit(ByteSize(byte_size));
                        }
                    }

                    response.events_sent().emit_event(events_sent);

                // This condition occurs specifically when the `HttpBatchService::call()` is called *within* the `Service::call()`
                } else if response.event_status() == EventStatus::Rejected {
                    Self::emit_call_error(None, request_id, event_count);
                    finalizers.update_status(EventStatus::Rejected);
                }
            }
        }
        drop(finalizers); // suppress "argument not consumed" warning
    }

    /// Emit the `Error` and `EventsDropped` internal events.
    /// This scenario occurs after retries have been attempted.
    fn emit_call_error(error: Option<Svc::Error>, request_id: usize, count: usize) {
        emit(CallError {
            error,
            request_id,
            count,
        });
    }
}

