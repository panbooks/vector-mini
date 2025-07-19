#![allow(missing_docs)]
use bytes::{Bytes, BytesMut};
use encoding_rs::{CoderResult, Encoding};

use crate::internal_events::{
    DecoderBomRemoval, DecoderMalformedReplacement, EncoderUnmappableReplacement,
};

const BUFFER_SIZE: usize = 4096;

// BOM unicode character (U+FEFF) expressed in utf-8
// http://unicode.org/faq/utf_bom.html#bom4
const BOM_UTF8: &[u8] = b"\xef\xbb\xbf";
const BOM_UTF8_LEN: usize = BOM_UTF8.len();

/// Helps transcoding from the specified encoding to utf8
pub struct Decoder {
    buffer: [u8; BUFFER_SIZE],
    output: BytesMut,
    inner: encoding_rs::Decoder,
}

impl Decoder {
    pub fn new(encoding: &'static Encoding) -> Self {
        Self {
            buffer: [0; BUFFER_SIZE],
            output: BytesMut::new(),
            // We explicitly choose not to remove BOM as part of encoding_rs's
            // decoding capabilities: the library has support for it, but it does
            // so only for the first input provided to the decoder (basically,
            // start of the stream), and for our usecases, we may get BOM markers
            // in later inputs too (eg: when reading multiple files):
            // https://docs.rs/encoding_rs/0.8.26/encoding_rs/struct.Encoding.html#method.new_decoder_with_bom_removal
            //
            // We can try to maintain separate decoders for each unique stream
            // (eg: by filepath when reading multiple files), but that mandates
            // cleanup of the initialized decoder structs/buffers when they are
            // no longer needed (eg: when files are closed), which can get
            // complicated. So we opt for simplicity here.
            //
            // BOM markers are still removed if the input starts with it:
            // see decode_to_utf8() for the rationale/logic.
            inner: encoding.new_decoder_without_bom_handling(),
        }
    }

    pub fn decode_to_utf8(&mut self, input: Bytes) -> Bytes {
        let mut total_read_from_input = 0;
        let mut total_had_errors = false;

        loop {
            let (result, read, written, had_errors) = self.inner.decode_to_utf8(
                &input[total_read_from_input..],
                &mut self.buffer,
                false, // not last (since we are processing a continuous stream)
            );

            total_read_from_input += read;
            total_had_errors |= had_errors;

            self.output.extend_from_slice(&self.buffer[..written]);

            match result {
                CoderResult::InputEmpty => break, // we have consumed all of the given input so we are done!
                CoderResult::OutputFull => (), // continue reading from the input in the next loop iteration
            }
        }

        if total_had_errors {
            emit!(DecoderMalformedReplacement {
                from_encoding: self.inner.encoding().name()
            });
        }

        let output = self.output.split().freeze();

        // All of the input (including any BOM sequences present) has been decoded
        // to utf-8 by now so we can check to see if the output starts with utf-8
        // BOM marker bytes and if it does, remove it for the final output.
        //
        // We can choose not to strip the BOM marker and keep it as is, but the
        // presence of these extra bytes can throw off any downstream processing
        // we do on the output, and rather than handling it specially on each
        // processing, we handle it centrally here. Also, the BOM does not serve
        // any more use for us, since the source encoding is already pre-identified
        // as part of decoder initialization.
        if output.get(..BOM_UTF8_LEN) == Some(BOM_UTF8) {
            emit!(DecoderBomRemoval {
                from_encoding: self.inner.encoding().name()
            });
            output.slice(BOM_UTF8_LEN..)
        } else {
            output
        }
    }
}

/// Helps transcoding to the specified encoding from utf8
pub struct Encoder {
    buffer: [u8; BUFFER_SIZE],
    output: BytesMut,
    inner: encoding_rs::Encoder,
    // Useful for tracking whether the encoder's encoding is utf-16 (and when it
    // is, its variety). Since encoding_rs does not have encoders for utf-16,
    // this is necessary:
    // https://docs.rs/encoding_rs/0.8.26/encoding_rs/index.html#utf-16le-utf-16be-and-unicode-encoding-schemes
    utf16_encoding: Option<Utf16Encoding>,
}

#[derive(Debug, Clone, Copy)]
enum Utf16Encoding {
    Le, // little-endian
    Be, // big-endian
}

impl Encoder {
    pub fn new(encoding: &'static Encoding) -> Self {
        Self {
            buffer: [0; BUFFER_SIZE],
            output: BytesMut::new(),
            inner: encoding.new_encoder(),
            utf16_encoding: Self::get_utf16_encoding(encoding),
        }
    }

    fn get_utf16_encoding(encoding: &'static Encoding) -> Option<Utf16Encoding> {
        match encoding.name() {
            "UTF-16LE" => Some(Utf16Encoding::Le),
            "UTF-16BE" => Some(Utf16Encoding::Be),
            _ => None,
        }
    }

    fn encode_from_utf8_to_utf16(&mut self, input: &str, variant: Utf16Encoding) -> Bytes {
        let to_bytes_func = match variant {
            Utf16Encoding::Le => u16::to_le_bytes,
            Utf16Encoding::Be => u16::to_be_bytes,
        };

        for utf16_value in input.encode_utf16() {
            self.output.extend_from_slice(&to_bytes_func(utf16_value));
        }

        self.output.split().freeze()
    }

    pub fn encode_from_utf8(&mut self, input: &str) -> Bytes {
        // alternate logic if the encoder is for a utf-16 encoding variant
        if let Some(variant) = self.utf16_encoding {
            return self.encode_from_utf8_to_utf16(input, variant);
        }

        let mut total_read_from_input = 0;
        let mut total_had_errors = false;

        loop {
            let (result, read, written, had_errors) = self.inner.encode_from_utf8(
                &input[total_read_from_input..],
                &mut self.buffer,
                false, // not last (since we are processing a continuous stream)
            );

            total_read_from_input += read;
            total_had_errors |= had_errors;

            self.output.extend_from_slice(&self.buffer[..written]);

            match result {
                CoderResult::InputEmpty => break, // we have consumed all of the given input so we are done!
                CoderResult::OutputFull => (), // continue reading from the input in the next loop iteration
            }
        }

        if total_had_errors {
            emit!(EncoderUnmappableReplacement {
                to_encoding: self.inner.encoding().name()
            });
        }

        self.output.split().freeze()
    }
}

