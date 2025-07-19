use crate::config::{DataType, TransformOutput};
use crate::event::{Event, LogEvent};
use std::collections::HashMap;

use indoc::indoc;
use vector_lib::transform::TransformOutputsBuf;

use crate::transforms::exclusive_route::config::{ExclusiveRouteConfig, UNMATCHED_ROUTE};
use crate::transforms::exclusive_route::transform::ExclusiveRoute;
use crate::transforms::SyncTransform;
use crate::{
    config::{build_unit_tests, ConfigBuilder},
    test_util::components::{init_test, COMPONENT_MULTIPLE_OUTPUTS_TESTS},
};

fn get_outputs_buf() -> (Vec<&'static str>, TransformOutputsBuf) {
    let names = vec!["a", "b", UNMATCHED_ROUTE];
    let buf = TransformOutputsBuf::new_with_capacity(
        names
            .iter()
            .map(|output_name| {
                TransformOutput::new(DataType::all_bits(), HashMap::new())
                    .with_port(output_name.to_owned())
            })
            .collect(),
        1,
    );
    (names, buf)
}


#[tokio::test]
async fn route_metrics_with_output_tag() {
    init_test();

    let config: ConfigBuilder = serde_yaml::from_str(indoc! {r#"
            transforms:
              foo:
                inputs: []
                type: "exclusive_route"
                routes:
                  - name: first
                    condition:
                      type: "is_log"

            tests:
              - name: "metric output"
                input:
                  insert_at: "foo"
                  value: "none"
                outputs:
                  - extract_from: "foo.first"
                    conditions:
                      - type: "vrl"
                        source: "true"
        "#})
    .unwrap();

    let mut tests = build_unit_tests(config).await.unwrap();
    assert!(tests.remove(0).run().await.errors.is_empty());
    // Check that metrics were emitted with output tag
    COMPONENT_MULTIPLE_OUTPUTS_TESTS.assert(&["output"]);
}
