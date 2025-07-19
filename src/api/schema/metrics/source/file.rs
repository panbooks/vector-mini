use std::{cmp::Ordering, collections::BTreeMap};

use async_graphql::{Enum, InputObject, Object};

use crate::{
    api::schema::{
        filter::{filter_items, CustomFilter, StringFilter},
        metrics::{self, MetricsFilter},
        relay, sort,
    },
    event::Metric,
    filter_check,
};

#[derive(Clone)]
pub struct FileSourceMetricFile<'a> {
    name: String,
    metrics: Vec<&'a Metric>,
}

impl<'a> FileSourceMetricFile<'a> {
    /// Returns a new FileSourceMetricFile from a (name, Vec<&Metric>) tuple
    #[allow(clippy::missing_const_for_fn)] // const cannot run destructor
    fn from_tuple((name, metrics): (String, Vec<&'a Metric>)) -> Self {
        Self { name, metrics }
    }

    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }
}

#[Object]
impl FileSourceMetricFile<'_> {
    /// File name
    async fn name(&self) -> &str {
        &*self.name
    }

    /// Metric indicating bytes received for the current file
    async fn received_bytes_total(&self) -> Option<metrics::ReceivedBytesTotal> {
        self.metrics.received_bytes_total()
    }

    /// Metric indicating received events for the current file
    async fn received_events_total(&self) -> Option<metrics::ReceivedEventsTotal> {
        self.metrics.received_events_total()
    }

    /// Metric indicating outgoing events for the current file
    async fn sent_events_total(&self) -> Option<metrics::SentEventsTotal> {
        self.metrics.sent_events_total()
    }
}

#[derive(Debug, Clone)]
pub struct FileSourceMetrics(Vec<Metric>);

impl FileSourceMetrics {
    pub const fn new(metrics: Vec<Metric>) -> Self {
        Self(metrics)
    }

    pub fn get_files(&self) -> Vec<FileSourceMetricFile<'_>> {
        self.0
            .iter()
            .filter_map(|m| m.tag_value("file").map(|file| (file, m)))
            .fold(
                BTreeMap::new(),
                |mut map: BTreeMap<String, Vec<&Metric>>, (file, m)| {
                    map.entry(file).or_default().push(m);
                    map
                },
            )
            .into_iter()
            .map(FileSourceMetricFile::from_tuple)
            .collect()
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum FileSourceMetricFilesSortFieldName {
    Name,
    ReceivedBytesTotal,
    ReceivedEventsTotal,
    SentEventsTotal,
}

impl sort::SortableByField<FileSourceMetricFilesSortFieldName> for FileSourceMetricFile<'_> {
    fn sort(&self, rhs: &Self, field: &FileSourceMetricFilesSortFieldName) -> Ordering {
        match field {
            FileSourceMetricFilesSortFieldName::Name => Ord::cmp(&self.name, &rhs.name),
            FileSourceMetricFilesSortFieldName::ReceivedBytesTotal => Ord::cmp(
                &self
                    .metrics
                    .received_bytes_total()
                    .map(|m| m.get_received_bytes_total() as i64)
                    .unwrap_or(0),
                &rhs.metrics
                    .received_bytes_total()
                    .map(|m| m.get_received_bytes_total() as i64)
                    .unwrap_or(0),
            ),
            FileSourceMetricFilesSortFieldName::ReceivedEventsTotal => Ord::cmp(
                &self
                    .metrics
                    .received_events_total()
                    .map(|m| m.get_received_events_total() as i64)
                    .unwrap_or(0),
                &rhs.metrics
                    .received_events_total()
                    .map(|m| m.get_received_events_total() as i64)
                    .unwrap_or(0),
            ),
            FileSourceMetricFilesSortFieldName::SentEventsTotal => Ord::cmp(
                &self
                    .metrics
                    .sent_events_total()
                    .map(|m| m.get_sent_events_total() as i64)
                    .unwrap_or(0),
                &rhs.metrics
                    .sent_events_total()
                    .map(|m| m.get_sent_events_total() as i64)
                    .unwrap_or(0),
            ),
        }
    }
}

#[derive(Default, InputObject)]
pub struct FileSourceMetricsFilesFilter {
    name: Option<Vec<StringFilter>>,
    or: Option<Vec<Self>>,
}

impl CustomFilter<FileSourceMetricFile<'_>> for FileSourceMetricsFilesFilter {
    fn matches(&self, file: &FileSourceMetricFile<'_>) -> bool {
        filter_check!(self
            .name
            .as_ref()
            .map(|f| f.iter().all(|f| f.filter_value(file.get_name()))));
        true
    }

    fn or(&self) -> Option<&Vec<Self>> {
        self.or.as_ref()
    }
}

#[allow(clippy::too_many_arguments)]
#[Object]
impl FileSourceMetrics {
    /// File metrics
    pub async fn files(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        filter: Option<FileSourceMetricsFilesFilter>,
        sort: Option<Vec<sort::SortField<FileSourceMetricFilesSortFieldName>>>,
    ) -> relay::ConnectionResult<FileSourceMetricFile<'_>> {
        let filter = filter.unwrap_or_default();
        let mut files = filter_items(self.get_files().into_iter(), &filter);

        if let Some(sort_fields) = sort {
            sort::by_fields(&mut files, &sort_fields);
        }

        relay::query(
            files.into_iter(),
            relay::Params::new(after, before, first, last),
            10,
        )
        .await
    }

    /// Total received bytes for the current file source
    pub async fn received_bytes_total(&self) -> Option<metrics::ReceivedBytesTotal> {
        self.0.received_bytes_total()
    }

    /// Total received events for the current file source
    pub async fn received_events_total(&self) -> Option<metrics::ReceivedEventsTotal> {
        self.0.received_events_total()
    }

    /// Total sent events for the current file source
    pub async fn sent_events_total(&self) -> Option<metrics::SentEventsTotal> {
        self.0.sent_events_total()
    }
}

