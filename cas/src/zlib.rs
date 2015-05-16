// An interface to a compression library.

use std::io::prelude::*;
use std::io::Cursor;
use flate2::{FlateReadExt, Compression};

// The old flate library provided some useful routines.  These are more
// taylored to the use by libpool.
// TODO: These should return Result rather than Option to convey a more
// meaningful error.

/// Attempt to compress a single block of data.  Returns the data if it is
/// compressible, otherwise, returns None.
pub fn deflate(buf: &[u8]) -> Option<Vec<u8>> {
    let src = Cursor::new(buf);
    let mut res = Vec::new();
    src.zlib_encode(Compression::Default).read_to_end(&mut res).unwrap();
    if res.len() < buf.len() {
        Some(res)
    } else {
        None
    }
}

/// Decompress the given buffer.  Returns None if there was some kind of error
/// doing the decompression.
pub fn inflate(buf: &[u8], size_hint: usize) -> Option<Vec<u8>> {
    let src = Cursor::new(buf);
    let mut res = Vec::with_capacity(size_hint);
    src.zlib_decode().read_to_end(&mut res).unwrap();
    if res.len() == size_hint {
        Some(res)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testutil::{boundary_sizes, make_random_string};

    fn check(len: u32) {
        let text = make_random_string(len, len).into_bytes();

        match deflate(&text[..]) {
            None => (),
            Some(ztext) => {
                match inflate(&ztext[..], text.len()) {
                    None => {
                        panic!("Unable to re-inflate compresed data");
                    },
                    Some(orig) => {
                        assert_eq!(text, orig);
                    }
                }
            },
        }
    }

    #[test]
    fn compressed() {
        for size in boundary_sizes() {
            check(size);
        }
    }
}
