use bytes::Bytes;
use rdkafka::message::{Header, OwnedHeaders};
use vector_lib::lookup::OwnedTargetPath;

use crate::{
    internal_events::KafkaHeaderExtractionError,
    sinks::{
        kafka::service::{KafkaRequest, KafkaRequestMetadata},
        prelude::*,
    },
};

pub struct KafkaRequestBuilder {
    pub key_field: Option<OwnedTargetPath>,
    pub headers_key: Option<OwnedTargetPath>,
    pub encoder: (Transformer, Encoder<()>),
}

impl RequestBuilder<(String, Event)> for KafkaRequestBuilder {
    type Metadata = KafkaRequestMetadata;
    type Events = Event;
    type Encoder = (Transformer, Encoder<()>);
    type Payload = Bytes;
    type Request = KafkaRequest;
    type Error = std::io::Error;

    fn compression(&self) -> Compression {
        Compression::None
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (String, Event),
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let (topic, mut event) = input;
        let builder = RequestMetadataBuilder::from_event(&event);

        let metadata = KafkaRequestMetadata {
            finalizers: event.take_finalizers(),
            key: get_key(&event, self.key_field.as_ref()),
            timestamp_millis: get_timestamp_millis(&event),
            headers: get_headers(&event, self.headers_key.as_ref()),
            topic,
        };

        (metadata, builder, event)
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        KafkaRequest {
            body: payload.into_payload(),
            metadata,
            request_metadata,
        }
    }
}

fn get_key(event: &Event, key_field: Option<&OwnedTargetPath>) -> Option<Bytes> {
    key_field.and_then(|key_field| match event {
        Event::Log(log) => log.get(key_field).map(|value| value.coerce_to_bytes()),
        Event::Metric(metric) => metric
            .tags()
            .and_then(|tags| tags.get(key_field.to_string().as_str()))
            .map(|value| value.to_owned().into()),
        _ => None,
    })
}

fn get_timestamp_millis(event: &Event) -> Option<i64> {
    match &event {
        Event::Log(log) => log.get_timestamp().and_then(|v| v.as_timestamp()).copied(),
        Event::Metric(metric) => metric.timestamp(),
        _ => None,
    }
    .map(|ts| ts.timestamp_millis())
}

fn get_headers(event: &Event, headers_key: Option<&OwnedTargetPath>) -> Option<OwnedHeaders> {
    headers_key.and_then(|headers_key| {
        if let Event::Log(log) = event {
            if let Some(headers) = log.get(headers_key) {
                match headers {
                    Value::Object(headers_map) => {
                        let mut owned_headers = OwnedHeaders::new_with_capacity(headers_map.len());
                        for (key, value) in headers_map {
                            if let Value::Bytes(value_bytes) = value {
                                owned_headers = owned_headers.insert(Header {
                                    key,
                                    value: Some(value_bytes.as_ref()),
                                });
                            } else {
                                emit!(KafkaHeaderExtractionError {
                                    header_field: headers_key
                                });
                            }
                        }
                        return Some(owned_headers);
                    }
                    _ => {
                        emit!(KafkaHeaderExtractionError {
                            header_field: headers_key
                        });
                    }
                }
            }
        }
        None
    })
}

