use super::LogEvent;

/// Encapsulates the inductive events merging algorithm.
///
/// In the future, this might be extended by various counters (the number of
/// events that contributed to the current merge event for instance, or the
/// event size) to support circuit breaker logic.
#[derive(Debug)]
pub struct LogEventMergeState {
    /// Intermediate event we merge into.
    intermediate_merged_event: LogEvent,
}

impl LogEventMergeState {
    /// Initialize the algorithm with a first (partial) event.
    pub fn new(first_partial_event: LogEvent) -> Self {
        Self {
            intermediate_merged_event: first_partial_event,
        }
    }

    /// Merge the incoming (partial) event in.
    pub fn merge_in_next_event(&mut self, incoming: LogEvent, fields: &[impl AsRef<str>]) {
        self.intermediate_merged_event.merge(incoming, fields);
    }

    /// Merge the final (non-partial) event in and return the resulting (merged)
    /// event.
    pub fn merge_in_final_event(
        mut self,
        incoming: LogEvent,
        fields: &[impl AsRef<str>],
    ) -> LogEvent {
        self.merge_in_next_event(incoming, fields);
        self.intermediate_merged_event
    }
}

