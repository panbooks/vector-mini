use std::collections::HashMap;

use vector_lib::lookup::path;
use vector_lib::{
    config::{LegacyKey, LogNamespace},
    event::Event,
};

use crate::sources::http_server::HttpConfigParamKind;

pub fn add_query_parameters(
    events: &mut [Event],
    query_parameters_config: &[HttpConfigParamKind],
    query_parameters: &HashMap<String, String>,
    log_namespace: LogNamespace,
    source_name: &'static str,
) {
    for qp in query_parameters_config {
        match qp {
            // Add each non-wildcard containing query_parameter that was specified
            // in the `query_parameters` config option to the event if an exact match
            // is found.
            HttpConfigParamKind::Exact(query_parameter_name) => {
                let value = query_parameters.get(query_parameter_name);

                for event in events.iter_mut() {
                    if let Event::Log(log) = event {
                        log_namespace.insert_source_metadata(
                            source_name,
                            log,
                            Some(LegacyKey::Overwrite(path!(query_parameter_name))),
                            path!("query_parameters", query_parameter_name),
                            crate::event::Value::from(value.map(String::to_owned)),
                        );
                    }
                }
            }
            // Add all query_parameters that match against wildcard pattens specified
            // in the `query_parameters` config option to the event.
            HttpConfigParamKind::Glob(query_parameter_pattern) => {
                for query_parameter_name in query_parameters.keys() {
                    if query_parameter_pattern
                        .matches_with(query_parameter_name.as_str(), glob::MatchOptions::default())
                    {
                        let value = query_parameters.get(query_parameter_name);

                        for event in events.iter_mut() {
                            if let Event::Log(log) = event {
                                log_namespace.insert_source_metadata(
                                    source_name,
                                    log,
                                    Some(LegacyKey::Overwrite(path!(query_parameter_name))),
                                    path!("query_parameters", query_parameter_name),
                                    crate::event::Value::from(value.map(String::to_owned)),
                                );
                            }
                        }
                    }
                }
            }
        };
    }
}

