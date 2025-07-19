//! Tests for the behaviour of Protobuf serializer and deserializer (together).

use bytes::{Bytes, BytesMut};
use std::path::{Path, PathBuf};
use tokio_util::codec::Encoder;
use vector_core::config::LogNamespace;

use codecs::decoding::format::Deserializer;
use codecs::decoding::{
    ProtobufDeserializer, ProtobufDeserializerConfig, ProtobufDeserializerOptions,
};
use codecs::encoding::{ProtobufSerializer, ProtobufSerializerConfig, ProtobufSerializerOptions};

fn test_data_dir() -> PathBuf {
    PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("tests/data/protobuf")
}

fn read_protobuf_bin_message(path: &Path) -> Bytes {
    let message_raw = std::fs::read(path).unwrap();
    Bytes::copy_from_slice(&message_raw)
}

/// Build the serializer and deserializer from common settings
fn build_serializer_pair(
    desc_file: PathBuf,
    message_type: String,
) -> (ProtobufSerializer, ProtobufDeserializer) {
    let serializer = ProtobufSerializerConfig {
        protobuf: ProtobufSerializerOptions {
            desc_file: desc_file.clone(),
            message_type: message_type.clone(),
        },
    }
    .build()
    .unwrap();
    let deserializer = ProtobufDeserializerConfig {
        protobuf: ProtobufDeserializerOptions {
            desc_file,
            message_type,
        },
    }
    .build()
    .unwrap();
    (serializer, deserializer)
}

