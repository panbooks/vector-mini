#![allow(missing_docs)]
#[allow(unused_imports)]
use std::collections::HashSet;

pub mod dedupe;
pub mod reduce;
#[cfg(feature = "transforms-impl-sample")]
pub mod sample;

#[cfg(feature = "transforms-aggregate")]
pub mod aggregate;
#[cfg(feature = "transforms-aws_ec2_metadata")]
pub mod aws_ec2_metadata;
#[cfg(feature = "transforms-exclusive-route")]
mod exclusive_route;
#[cfg(feature = "transforms-filter")]
pub mod filter;
#[cfg(feature = "transforms-log_to_metric")]
pub mod log_to_metric;
#[cfg(feature = "transforms-lua")]
pub mod lua;
#[cfg(feature = "transforms-metric_to_log")]
pub mod metric_to_log;
#[cfg(feature = "transforms-remap")]
pub mod remap;
#[cfg(feature = "transforms-route")]
pub mod route;
#[cfg(feature = "transforms-tag_cardinality_limit")]
pub mod tag_cardinality_limit;
#[cfg(feature = "transforms-throttle")]
pub mod throttle;
#[cfg(feature = "transforms-window")]
pub mod window;

pub use vector_lib::transform::{
    FunctionTransform, OutputBuffer, SyncTransform, TaskTransform, Transform, TransformOutputs,
    TransformOutputsBuf,
};
