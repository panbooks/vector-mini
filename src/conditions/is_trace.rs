use crate::event::Event;

pub(crate) const fn check_is_trace(e: Event) -> (bool, Event) {
    (matches!(e, Event::Trace(_)), e)
}

pub(crate) fn check_is_trace_with_context(e: Event) -> (Result<(), String>, Event) {
    let (result, event) = check_is_trace(e);
    if result {
        (Ok(()), event)
    } else {
        (Err("event is not a trace type".to_string()), event)
    }
}

