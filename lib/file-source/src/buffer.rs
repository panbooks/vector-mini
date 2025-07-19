use std::{
    cmp::min,
    io::{self, BufRead},
};

use bstr::Finder;
use bytes::BytesMut;

use crate::FilePosition;

pub struct ReadResult {
    pub successfully_read: Option<usize>,
    pub discarded_for_size_and_truncated: Vec<BytesMut>,
}

/// Read up to `max_size` bytes from `reader`, splitting by `delim`
///
/// The function reads up to `max_size` bytes from `reader`, splitting the input
/// by `delim`. If a `delim` is not found in `reader` before `max_size` bytes
/// are read then the reader is polled until `delim` is found and the results
/// are discarded. Else, the result is written into `buf`.
///
/// The return is unusual. In the Err case this function has not written into
/// `buf` and the caller should not examine its contents. In the Ok case if the
/// inner value is None the caller should retry the call as the buffering read
/// hit the end of the buffer but did not find a `delim` yet, indicating that
/// we've sheered a write or that there were no bytes available in the `reader`
/// and the `reader` was very sure about it. If the inner value is Some the
/// interior `usize` is the number of bytes written into `buf`.
///
/// Tweak of
/// <https://github.com/rust-lang/rust/blob/bf843eb9c2d48a80a5992a5d60858e27269f9575/src/libstd/io/mod.rs#L1471>.
///
/// # Performance
///
/// Benchmarks indicate that this function processes in the high single-digit
/// GiB/s range for buffers of length 1KiB. For buffers any smaller than this
/// the overhead of setup dominates our benchmarks.
pub fn read_until_with_max_size<'a, R: BufRead + ?Sized>(
    reader: &'a mut R,
    position: &'a mut FilePosition,
    delim: &'a [u8],
    buf: &'a mut BytesMut,
    max_size: usize,
) -> io::Result<ReadResult> {
    let mut total_read = 0;
    let mut discarding = false;
    let delim_finder = Finder::new(delim);
    let delim_len = delim.len();
    let mut discarded_for_size_and_truncated = Vec::new();
    loop {
        let available: &[u8] = match reader.fill_buf() {
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        let (done, used) = {
            match delim_finder.find(available) {
                Some(i) => {
                    if !discarding {
                        buf.extend_from_slice(&available[..i]);
                    }
                    (true, i + delim_len)
                }
                None => {
                    if !discarding {
                        buf.extend_from_slice(available);
                    }
                    (false, available.len())
                }
            }
        };
        reader.consume(used);
        *position += used as u64; // do this at exactly same time
        total_read += used;

        if !discarding && buf.len() > max_size {
            // keep only the first <1k bytes to make sure we can actually emit a usable error
            let length_to_keep = min(1000, max_size);
            let mut truncated: BytesMut = BytesMut::zeroed(length_to_keep);
            truncated.copy_from_slice(&buf[0..length_to_keep]);
            discarded_for_size_and_truncated.push(truncated);
            discarding = true;
        }

        if done {
            if !discarding {
                return Ok(ReadResult {
                    successfully_read: Some(total_read),
                    discarded_for_size_and_truncated,
                });
            } else {
                discarding = false;
                buf.clear();
            }
        } else if used == 0 {
            // We've hit EOF but not yet seen a newline. This can happen when unlucky timing causes
            // us to observe an incomplete write. We return None here and let the loop continue
            // next time the method is called. This is safe because the buffer is specific to this
            // FileWatcher.
            return Ok(ReadResult {
                successfully_read: None,
                discarded_for_size_and_truncated,
            });
        }
    }
}

