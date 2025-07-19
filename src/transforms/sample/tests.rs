use crate::template::Template;
use crate::test_util::components::assert_transform_compliance;
use crate::transforms::sample::config::SampleConfig;
use crate::transforms::test::create_topology;
use crate::transforms::{FunctionTransform, OutputBuffer};
use crate::{
    conditions::{Condition, ConditionalConfig, VrlConfig},
    config::log_schema,
    event::{Event, LogEvent, TraceEvent},
    test_util::random_lines,
    transforms::sample::config::default_sample_rate_key,
    transforms::sample::transform::{Sample, SampleMode},
    transforms::test::transform_one,
};
use approx::assert_relative_eq;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use vector_lib::lookup::lookup_v2::OptionalValuePath;
use vrl::owned_value_path;

#[tokio::test]
async fn emits_internal_events() {
    assert_transform_compliance(async move {
        let config = SampleConfig {
            rate: None,
            ratio: Some(1.0),
            key_field: None,
            group_by: None,
            exclude: None,
            sample_rate_key: default_sample_rate_key(),
        };
        let (tx, rx) = mpsc::channel(1);
        let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

        let log = LogEvent::from("hello world");
        tx.send(log.into()).await.unwrap();

        _ = out.recv().await;

        drop(tx);
        topology.stop().await;
        assert_eq!(out.recv().await, None);
    })
    .await
}









fn condition_contains(key: &str, needle: &str) -> Condition {
    let vrl_config = VrlConfig {
        source: format!(r#"contains!(."{key}", "{needle}")"#),
        runtime: Default::default(),
    };

    vrl_config
        .build(&Default::default())
        .expect("should not fail to build VRL condition")
}

fn random_events(n: usize) -> Vec<Event> {
    random_lines(10)
        .take(n)
        .map(|e| Event::Log(LogEvent::from(e)))
        .collect()
}
