// Kinds

//! Kinds.
//!
//! Kinds are basically fourcc strings with the local endianness.

#![macro_escape]

use core::raw::Slice;
use std::mem;
use std::fmt;
use std::slice::bytes;

#[derive(PartialEq, Copy)]
pub struct Kind {
    pub raw: u32
}

// TODO: The 'Kind' reference below doesn't seem to have proper hygiene.
#[macro_export]
macro_rules! kind(
    ($k:expr) => (
        Kind {
            raw: fourcc!($k, target)
        }
    );
);

impl Kind {
    // View as byte array.
    pub fn as_bytes<'a>(&self) -> &'a [u8] {
        unsafe {
            mem::transmute(Slice {
                data: &self.raw,
                len: 4,
            })
        }
    }

    pub fn as_mut_bytes<'a>(&mut self) -> &'a mut [u8] {
        unsafe {
            mem::transmute(Slice {
                data: &self.raw,
                len: 4,
            })
        }
    }

    // This is from when lifetimes didn't work as well.
    #[deprecated = "use `.as_bytes()` instead"]
    pub fn to_bytes<U>(self, f: |v: &[u8]| -> U) -> U {
        let buf: &[u8] = unsafe {
            mem::transmute(Slice { data: &self.raw, len: 4 })
        };
        f(buf)
    }

    pub fn from_str(text: &str) -> Option<Kind> {
        if text.len() != 4 { return None; }
        let mut result: Kind = unsafe { mem::uninitialized() };
        bytes::copy_memory(result.as_mut_bytes(), text.as_bytes());

        Some(result)
    }

    // Return a new vector (of length 4) containing the
    // representation of this kind.
    pub fn bytes(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(4);
        result.push_all(self.as_bytes());
        result
    }

    // This isn't 'ToStr' to integrate better with fmt.
    pub fn textual(&self) -> String {
        String::from_utf8(self.clone().bytes()).unwrap_or_else(|_| "????".to_string())
    }
}

impl fmt::Show for Kind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "'{}'", self.textual())
    }
}

#[test]
fn kinds() {
    // println!("{:#8x}", kind!("blob").raw);
    // println!("{}", kind!("blob").bytes());
    assert!(kind!("blob").bytes() == vec!(98, 108, 111, 98));
    assert!(kind!("abcd").bytes() == vec!(97, 98, 99, 100));

    assert_eq!(kind!("blob").bytes(), Kind::from_str("blob").unwrap().bytes());
}
