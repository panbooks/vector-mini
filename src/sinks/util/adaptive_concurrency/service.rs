use std::{
    fmt,
    future::Future,
    mem,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures::future::BoxFuture;
use tokio::sync::OwnedSemaphorePermit;
use tower::{load::Load, Service};

use super::{controller::Controller, future::ResponseFuture, AdaptiveConcurrencySettings};
use crate::sinks::util::retries::RetryLogic;

/// Enforces a limit on the concurrent number of requests the underlying
/// service can handle. Automatically expands and contracts the actual
/// concurrency limit depending on observed request response behavior.
pub struct AdaptiveConcurrencyLimit<S, L> {
    inner: S,
    pub(super) controller: Arc<Controller<L>>,
    state: State,
}

enum State {
    Waiting(BoxFuture<'static, OwnedSemaphorePermit>),
    Ready(OwnedSemaphorePermit),
    Empty,
}

impl<S, L> AdaptiveConcurrencyLimit<S, L> {
    /// Create a new automated concurrency limiter.
    pub(crate) fn new(
        inner: S,
        logic: L,
        concurrency: Option<usize>,
        options: AdaptiveConcurrencySettings,
    ) -> Self {
        AdaptiveConcurrencyLimit {
            inner,
            controller: Arc::new(Controller::new(concurrency, options, logic)),
            state: State::Empty,
        }
    }
}

impl<S, L, Request> Service<Request> for AdaptiveConcurrencyLimit<S, L>
where
    S: Service<Request>,
    S::Error: Into<crate::Error>,
    L: RetryLogic<Response = S::Response>,
{
    type Response = S::Response;
    type Error = crate::Error;
    type Future = ResponseFuture<S::Future, L>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            self.state = match self.state {
                State::Ready(_) => return self.inner.poll_ready(cx).map_err(Into::into),
                State::Waiting(ref mut fut) => {
                    tokio::pin!(fut);
                    let permit = ready!(fut.poll(cx));
                    State::Ready(permit)
                }
                State::Empty => State::Waiting(Box::pin(Arc::clone(&self.controller).acquire())),
            };
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Make sure a permit has been acquired
        let permit = match mem::replace(&mut self.state, State::Empty) {
            // Take the permit.
            State::Ready(permit) => permit,
            // whoopsie!
            _ => panic!("Maximum requests in-flight; poll_ready must be called first"),
        };

        self.controller.start_request();

        // Call the inner service
        let future = self.inner.call(request);

        ResponseFuture::new(future, permit, Arc::clone(&self.controller))
    }
}

impl<S, L> Load for AdaptiveConcurrencyLimit<S, L> {
    type Metric = f64;

    fn load(&self) -> Self::Metric {
        self.controller.load()
    }
}

impl<S, L> Clone for AdaptiveConcurrencyLimit<S, L>
where
    S: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            controller: Arc::clone(&self.controller),
            state: State::Empty,
        }
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Waiting(_) => f
                .debug_tuple("State::Waiting")
                .field(&format_args!("..."))
                .finish(),
            State::Ready(r) => f.debug_tuple("State::Ready").field(&r).finish(),
            State::Empty => f.debug_tuple("State::Empty").finish(),
        }
    }
}

