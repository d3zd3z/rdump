// Kinds

//! Kinds.
//!
//! Kinds are basically fourcc strings with the local endianness.

#![macro_escape]

use core::raw::Slice;
use std::mem;

#[deriving(PartialEq)]
pub struct Kind {
    pub raw: u32
}

#[macro_export]
macro_rules! kind(
    ($k:expr) => (
        ::kind::Kind {
            raw: fourcc!($k, target)
        }
    );
)

impl Kind {
    // View as byte array.
    pub fn to_bytes<U>(self, f: |v: &[u8]| -> U) -> U {
        let buf: &[u8] = unsafe {
            mem::transmute(Slice { data: &self.raw, len: 4 })
        };
        f(buf)
    }

    // Return a new vector (of length 4) containing the
    // representation of this kind.
    pub fn bytes(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(4);
        self.to_bytes(|v| result.push_all(v));
        result
    }

    // This isn't 'ToStr' to integrate better with fmt.
    pub fn textual(&self) -> String {
        String::from_utf8(self.bytes()).unwrap_or_else(|_| "????".to_string())
    }
}

#[test]
fn kinds() {
    // println!("{:#8x}", kind!("blob").raw);
    // println!("{}", kind!("blob").bytes());
    assert!(kind!("blob").bytes() == vec!(98, 108, 111, 98));
    assert!(kind!("abcd").bytes() == vec!(97, 98, 99, 100));
}
