use std::collections::VecDeque;

use vector_lib::internal_event::{Count, InternalEventHandle as _, Registered};

use crate::{
    conditions::Condition,
    event::Event,
    internal_events::WindowEventsDropped,
    transforms::{FunctionTransform, OutputBuffer},
};

#[derive(Clone)]
pub struct Window {
    // Configuration parameters
    forward_when: Option<Condition>,
    flush_when: Condition,
    num_events_before: usize,
    num_events_after: usize,

    // Internal variables
    buffer: VecDeque<Event>,
    events_counter: usize,
    events_dropped: Registered<WindowEventsDropped>,
    is_flushing: bool,
}

impl Window {
    pub fn new(
        forward_when: Option<Condition>,
        flush_when: Condition,
        num_events_before: usize,
        num_events_after: usize,
    ) -> crate::Result<Self> {
        let buffer = VecDeque::with_capacity(num_events_before);

        Ok(Window {
            forward_when,
            flush_when,
            num_events_before,
            num_events_after,
            events_dropped: register!(WindowEventsDropped),
            buffer,
            events_counter: 0,
            is_flushing: false,
        })
    }
}

impl FunctionTransform for Window {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let (pass, event) = match self.forward_when.as_ref() {
            Some(condition) => {
                let (result, event) = condition.check(event);
                (result, event)
            }
            _ => (false, event),
        };

        let (flush, event) = self.flush_when.check(event);

        if self.buffer.capacity() < self.num_events_before {
            self.buffer.reserve(self.num_events_before);
        }

        if pass {
            output.push(event);
        } else if flush {
            if self.num_events_before > 0 {
                self.buffer.drain(..).for_each(|evt| output.push(evt));
            }

            self.events_counter = 0;
            self.is_flushing = true;
            output.push(event);
        } else if self.is_flushing {
            self.events_counter += 1;

            if self.events_counter > self.num_events_after {
                self.events_counter = 0;
                self.is_flushing = false;
                self.events_dropped.emit(Count(1));
            } else {
                output.push(event);
            }
        } else if self.buffer.len() >= self.num_events_before {
            self.buffer.pop_front();
            self.buffer.push_back(event);
            self.events_dropped.emit(Count(1));
        } else if self.num_events_before > 0 {
            self.buffer.push_back(event);
        } else {
            self.events_dropped.emit(Count(1));
        }
    }
}

