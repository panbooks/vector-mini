use bytes::Bytes;
use vector_lib::lookup::path;
use vector_lib::{
    config::{LegacyKey, LogNamespace},
    event::Event,
};
use warp::http::{HeaderMap, HeaderValue};

use crate::event::Value;
use crate::sources::http_server::HttpConfigParamKind;

pub fn add_headers(
    events: &mut [Event],
    headers_config: &[HttpConfigParamKind],
    headers: &HeaderMap,
    log_namespace: LogNamespace,
    source_name: &'static str,
) {
    for h in headers_config {
        match h {
            // Add each non-wildcard containing header that was specified
            // in the `headers` config option to the event if an exact match
            // is found.
            HttpConfigParamKind::Exact(header_name) => {
                let value = headers.get(header_name).map(HeaderValue::as_bytes);

                for event in events.iter_mut() {
                    if let Event::Log(log) = event {
                        log_namespace.insert_source_metadata(
                            source_name,
                            log,
                            Some(LegacyKey::InsertIfEmpty(path!(header_name))),
                            path!("headers", header_name),
                            Value::from(value.map(Bytes::copy_from_slice)),
                        );
                    }
                }
            }
            // Add all headers that match against wildcard pattens specified
            // in the `headers` config option to the event.
            HttpConfigParamKind::Glob(header_pattern) => {
                for header_name in headers.keys() {
                    if header_pattern
                        .matches_with(header_name.as_str(), glob::MatchOptions::default())
                    {
                        let value = headers.get(header_name).map(HeaderValue::as_bytes);

                        for event in events.iter_mut() {
                            if let Event::Log(log) = event {
                                log_namespace.insert_source_metadata(
                                    source_name,
                                    log,
                                    Some(LegacyKey::InsertIfEmpty(path!(header_name.as_str()))),
                                    path!("headers", header_name.as_str()),
                                    Value::from(value.map(Bytes::copy_from_slice)),
                                );
                            }
                        }
                    }
                }
            }
        };
    }
}

