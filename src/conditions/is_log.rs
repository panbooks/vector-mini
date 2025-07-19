use crate::event::Event;

pub(crate) const fn check_is_log(e: Event) -> (bool, Event) {
    (matches!(e, Event::Log(_)), e)
}

pub(crate) fn check_is_log_with_context(e: Event) -> (Result<(), String>, Event) {
    let (result, event) = check_is_log(e);
    if result {
        (Ok(()), event)
    } else {
        (Err("event is not a log type".to_string()), event)
    }
}

