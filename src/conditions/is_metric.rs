use crate::event::Event;

pub(crate) const fn check_is_metric(e: Event) -> (bool, Event) {
    (matches!(e, Event::Metric(_)), e)
}

pub(crate) fn check_is_metric_with_context(e: Event) -> (Result<(), String>, Event) {
    let (result, event) = check_is_metric(e);
    if result {
        (Ok(()), event)
    } else {
        (Err("event is not a metric type".to_string()), event)
    }
}

