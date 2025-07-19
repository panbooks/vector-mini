use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder;
use vector_core::{config::DataType, event::Event, schema};

/// Config used to build a `NativeJsonSerializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct NativeJsonSerializerConfig;

impl NativeJsonSerializerConfig {
    /// Build the `NativeJsonSerializer` from this configuration.
    pub const fn build(&self) -> NativeJsonSerializer {
        NativeJsonSerializer
    }

    /// The data type of events that are accepted by `NativeJsonSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::all_bits()
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the JSON format.
#[derive(Debug, Clone)]
pub struct NativeJsonSerializer;

impl NativeJsonSerializer {
    /// Encode event and represent it as native JSON value.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, vector_common::Error> {
        serde_json::to_value(&event).map_err(|e| e.to_string().into())
    }
}

impl Encoder<Event> for NativeJsonSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let writer = buffer.writer();
        serde_json::to_writer(writer, &event).map_err(Into::into)
    }
}

