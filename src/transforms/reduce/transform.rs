use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};

use crate::internal_events::ReduceAddEventError;
use crate::transforms::reduce::merge_strategy::{
    get_value_merger, MergeStrategy, ReduceValueMerger,
};
use crate::{
    conditions::Condition,
    event::{discriminant::Discriminant, Event, EventMetadata, LogEvent},
    internal_events::ReduceStaleEventFlushed,
    transforms::{reduce::config::ReduceConfig, TaskTransform},
};
use futures::Stream;
use indexmap::IndexMap;
use vector_lib::stream::expiration_map::{map_with_expiration, Emitter};
use vrl::path::{parse_target_path, OwnedTargetPath};
use vrl::prelude::KeyString;

#[derive(Clone, Debug)]
struct ReduceState {
    events: usize,
    fields: HashMap<OwnedTargetPath, Box<dyn ReduceValueMerger>>,
    stale_since: Instant,
    creation: Instant,
    metadata: EventMetadata,
}

fn is_covered_by_strategy(
    path: &OwnedTargetPath,
    strategies: &IndexMap<OwnedTargetPath, MergeStrategy>,
) -> bool {
    let mut current = OwnedTargetPath::event_root();
    for component in &path.path.segments {
        current = current.with_field_appended(&component.to_string());
        if strategies.contains_key(&current) {
            return true;
        }
    }
    false
}

impl ReduceState {
    fn new() -> Self {
        Self {
            events: 0,
            stale_since: Instant::now(),
            creation: Instant::now(),
            fields: HashMap::new(),
            metadata: EventMetadata::default(),
        }
    }

    fn add_event(&mut self, e: LogEvent, strategies: &IndexMap<OwnedTargetPath, MergeStrategy>) {
        self.metadata.merge(e.metadata().clone());

        for (path, strategy) in strategies {
            if let Some(value) = e.get(path) {
                match self.fields.entry(path.clone()) {
                    Entry::Vacant(entry) => match get_value_merger(value.clone(), strategy) {
                        Ok(m) => {
                            entry.insert(m);
                        }
                        Err(error) => {
                            warn!(message = "Failed to create value merger.", %error, %path);
                        }
                    },
                    Entry::Occupied(mut entry) => {
                        if let Err(error) = entry.get_mut().add(value.clone()) {
                            warn!(message = "Failed to merge value.", %error);
                        }
                    }
                }
            }
        }

        if let Some(fields_iter) = e.all_event_fields_skip_array_elements() {
            for (path, value) in fields_iter {
                // This should not return an error, unless there is a bug in the event fields iterator.
                let parsed_path = match parse_target_path(&path) {
                    Ok(path) => path,
                    Err(error) => {
                        emit!(ReduceAddEventError { error, path });
                        continue;
                    }
                };
                if is_covered_by_strategy(&parsed_path, strategies) {
                    continue;
                }

                let maybe_strategy = strategies.get(&parsed_path);
                match self.fields.entry(parsed_path) {
                    Entry::Vacant(entry) => {
                        if let Some(strategy) = maybe_strategy {
                            match get_value_merger(value.clone(), strategy) {
                                Ok(m) => {
                                    entry.insert(m);
                                }
                                Err(error) => {
                                    warn!(message = "Failed to merge value.", %error);
                                }
                            }
                        } else {
                            entry.insert(value.clone().into());
                        }
                    }
                    Entry::Occupied(mut entry) => {
                        if let Err(error) = entry.get_mut().add(value.clone()) {
                            warn!(message = "Failed to merge value.", %error);
                        }
                    }
                }
            }
        }
        // else the event root is not an object (see https://github.com/vectordotdev/vector/issues/18219)

        self.events += 1;
        self.stale_since = Instant::now();
    }

    fn flush(mut self) -> LogEvent {
        let mut event = LogEvent::new_with_metadata(self.metadata);
        for (path, v) in self.fields.drain() {
            if let Err(error) = v.insert_into(&path, &mut event) {
                warn!(message = "Failed to merge values for field.", %error);
            }
        }
        self.events = 0;
        event
    }
}

#[derive(Clone, Debug)]
pub struct Reduce {
    expire_after: Duration,
    flush_period: Duration,
    end_every_period: Option<Duration>,
    group_by: Vec<String>,
    merge_strategies: IndexMap<OwnedTargetPath, MergeStrategy>,
    reduce_merge_states: HashMap<Discriminant, ReduceState>,
    ends_when: Option<Condition>,
    starts_when: Option<Condition>,
    max_events: Option<usize>,
}

fn validate_merge_strategies(strategies: IndexMap<KeyString, MergeStrategy>) -> crate::Result<()> {
    for (path, _) in &strategies {
        let contains_index = parse_target_path(path)
            .map_err(|_| format!("Could not parse path: `{path}`"))?
            .path
            .segments
            .iter()
            .any(|segment| segment.is_index());
        if contains_index {
            return Err(format!(
                "Merge strategies with indexes are currently not supported. Path: `{path}`"
            )
            .into());
        }
    }

    Ok(())
}

impl Reduce {
    pub fn new(
        config: &ReduceConfig,
        enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) -> crate::Result<Self> {
        if config.ends_when.is_some() && config.starts_when.is_some() {
            return Err("only one of `ends_when` and `starts_when` can be provided".into());
        }

        let ends_when = config
            .ends_when
            .as_ref()
            .map(|c| c.build(enrichment_tables))
            .transpose()?;
        let starts_when = config
            .starts_when
            .as_ref()
            .map(|c| c.build(enrichment_tables))
            .transpose()?;
        let group_by = config.group_by.clone().into_iter().collect();
        let max_events = config.max_events.map(|max| max.into());

        validate_merge_strategies(config.merge_strategies.clone())?;

        Ok(Reduce {
            expire_after: config.expire_after_ms,
            flush_period: config.flush_period_ms,
            end_every_period: config.end_every_period_ms,
            group_by,
            merge_strategies: config
                .merge_strategies
                .iter()
                .filter_map(|(path, strategy)| {
                    // TODO Invalid paths are ignored to preserve backwards compatibility.
                    //      Merge strategy paths should ideally be [`lookup_v2::ConfigTargetPath`]
                    //      which means an invalid path would result in an configuration error.
                    let parsed_path = parse_target_path(path).ok();
                    if parsed_path.is_none() {
                        warn!(message = "Ignoring strategy with invalid path.", %path);
                    }
                    parsed_path.map(|path| (path, strategy.clone()))
                })
                .collect(),
            reduce_merge_states: HashMap::new(),
            ends_when,
            starts_when,
            max_events,
        })
    }

    fn flush_into(&mut self, emitter: &mut Emitter<Event>) {
        let mut flush_discriminants = Vec::new();
        let now = Instant::now();
        for (k, t) in &self.reduce_merge_states {
            if let Some(period) = self.end_every_period {
                if (now - t.creation) >= period {
                    flush_discriminants.push(k.clone());
                }
            }

            if (now - t.stale_since) >= self.expire_after {
                flush_discriminants.push(k.clone());
            }
        }
        for k in &flush_discriminants {
            if let Some(t) = self.reduce_merge_states.remove(k) {
                emit!(ReduceStaleEventFlushed);
                emitter.emit(Event::from(t.flush()));
            }
        }
    }

    fn flush_all_into(&mut self, emitter: &mut Emitter<Event>) {
        self.reduce_merge_states
            .drain()
            .for_each(|(_, s)| emitter.emit(Event::from(s.flush())));
    }

    fn push_or_new_reduce_state(&mut self, event: LogEvent, discriminant: Discriminant) {
        match self.reduce_merge_states.entry(discriminant) {
            Entry::Vacant(entry) => {
                let mut state = ReduceState::new();
                state.add_event(event, &self.merge_strategies);
                entry.insert(state);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().add_event(event, &self.merge_strategies);
            }
        };
    }

    pub fn transform_one(&mut self, emitter: &mut Emitter<Event>, event: Event) {
        let (starts_here, event) = match &self.starts_when {
            Some(condition) => condition.check(event),
            None => (false, event),
        };

        let (mut ends_here, event) = match &self.ends_when {
            Some(condition) => condition.check(event),
            None => (false, event),
        };

        let event = event.into_log();
        let discriminant = Discriminant::from_log_event(&event, &self.group_by);

        if let Some(max_events) = self.max_events {
            if max_events == 1 {
                ends_here = true;
            } else if let Some(entry) = self.reduce_merge_states.get(&discriminant) {
                // The current event will finish this set
                if entry.events + 1 == max_events {
                    ends_here = true;
                }
            }
        }

        if starts_here {
            if let Some(state) = self.reduce_merge_states.remove(&discriminant) {
                emitter.emit(state.flush().into());
            }

            self.push_or_new_reduce_state(event, discriminant)
        } else if ends_here {
            emitter.emit(match self.reduce_merge_states.remove(&discriminant) {
                Some(mut state) => {
                    state.add_event(event, &self.merge_strategies);
                    state.flush().into()
                }
                None => {
                    let mut state = ReduceState::new();
                    state.add_event(event, &self.merge_strategies);
                    state.flush().into()
                }
            });
        } else {
            self.push_or_new_reduce_state(event, discriminant)
        }
    }
}

impl TaskTransform<Event> for Reduce {
    fn transform(
        self: Box<Self>,
        input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let transform_fn = move |me: &mut Box<Reduce>, event, emitter: &mut Emitter<Event>| {
            me.transform_one(emitter, event);
        };

        construct_output_stream(self, input_rx, transform_fn)
    }
}

pub fn construct_output_stream(
    reduce: Box<Reduce>,
    input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    mut transform_fn: impl FnMut(&mut Box<Reduce>, Event, &mut Emitter<Event>) + Send + Sync + 'static,
) -> Pin<Box<dyn Stream<Item = Event> + Send>>
where
    Reduce: 'static,
{
    let flush_period = reduce.flush_period;
    Box::pin(map_with_expiration(
        reduce,
        input_rx,
        flush_period,
        move |me, event, emitter| {
            transform_fn(me, event, emitter);
        },
        |me, emitter| {
            me.flush_into(emitter);
        },
        |me, emitter| {
            me.flush_all_into(emitter);
        },
    ))
}
