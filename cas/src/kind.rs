// Backup kinds

//! A Kind is a u32 that corresponds to a 4-character ASCII string.

use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use super::Error;
use super::Result;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Kind {
    pub raw: u32,
}

impl Kind {
    pub fn new(text: &str) -> Result<Kind> {
        let b = text.as_bytes();
        if b.len() != text.chars().count() {
            return Err(Error::NonAsciiKind);
        }
        if b.len() != 4 {
            return Err(Error::BadKindLength);
        }

        let mut rd = Cursor::new(text.as_bytes());
        Ok(Kind {
            raw: try!(rd.read_u32::<LittleEndian>()),
        })
    }

    // Get the kind back as bytes.
    pub fn bytes(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(4);
        result.write_u32::<LittleEndian>(self.raw).unwrap();
        result
    }
}

impl ToString for Kind {
    fn to_string(&self) -> String {
        String::from_utf8(self.bytes()).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use ::Error;

    macro_rules! assert_err {
        ( $test:expr, $exp:path) => {
            match $test {
                Err($exp) => (),
                ref err => panic!("Unexpected error: {:?}, expecting {:?}", err, $exp),
            }
        }
    }

    #[test]
    fn test_new() {
        assert_eq!(Kind::new("blob").unwrap(), Kind { raw: 0x626f6c62 });

        assert_err!(Kind::new("bloby"), Error::BadKindLength);
        assert_err!(Kind::new("blo"), Error::BadKindLength);
        assert_err!(Kind::new("b\u{2022}b"), Error::NonAsciiKind);
        assert_err!(Kind::new("bl\u{2022}b"), Error::NonAsciiKind);
        assert_err!(Kind::new("blo\u{2022}b"), Error::NonAsciiKind);
    }

    #[test]
    fn test_bytes() {
        assert_eq!(Kind::new("blob").unwrap().bytes(),
            &[0x62, 0x6c, 0x6f, 0x62]);
    }

    #[test]
    fn test_string() {
        assert_eq!(Kind::new("blob").unwrap().to_string(), "blob");
    }
}
