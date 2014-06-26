// Object ID.

//! Object IDs.
//!
//! Every object in the archive is identified by an object-id (OID)
//! which is the SHA-1 hash of the 'kind' followed by the payload
//! itself.

use std::fmt;
use std::mem;

use kind::Kind;

pub struct Oid {
    pub bytes: [u8, ..20],
}

impl PartialEq for Oid {
    fn eq(&self, other: &Oid) -> bool {
        self.bytes == other.bytes
    }
}

impl Clone for Oid {
    fn clone(&self) -> Oid {
        let mut result: Oid = unsafe { mem::uninitialized() };
        result.bytes = self.bytes;
        result
    }
}

mod openssl {
    use libc::{c_int, c_uint, c_uchar, c_void, size_t, uint32_t};
    use std::mem;

    // Despite the type name in the SSL header, these are expected to
    // all be 32-bit values.
    pub struct ShaCtx {
        _h0: uint32_t,
        _h1: uint32_t,
        _h2: uint32_t,
        _h3: uint32_t,
        _h4: uint32_t,
        _nl: uint32_t,
        _nh: uint32_t,
        _data: [uint32_t, ..16],
        _num: c_uint,
    }

    #[link(name = "crypto")]
    extern {
        pub fn SHA1_Init(c: *mut ShaCtx) -> c_int;
        pub fn SHA1_Update(c: *mut ShaCtx, data: *c_void, len: size_t) -> c_int;
        pub fn SHA1_Final(md: *mut c_uchar, c: *mut ShaCtx) -> c_int;
    }

    #[test]
    fn context_size() {
        assert!(mem::size_of::<ShaCtx>() == 96);
    }
}

pub struct Context {
    core: openssl::ShaCtx,
}

impl Context {
    fn init() -> Context {
        unsafe {
            let mut result: Context = mem::uninitialized();
            openssl::SHA1_Init(&mut result.core);
            result
        }
    }

    fn update(&mut self, data: &[u8]) {
        unsafe {
            openssl::SHA1_Update(&mut self.core,
                                 data.as_ptr() as *::libc::c_void,
                                 data.len() as ::libc::size_t);
        }
    }

    fn final(&mut self) -> Oid {
        unsafe {
            let mut result: Oid = mem::uninitialized();
            openssl::SHA1_Final(&mut result.bytes[0], &mut self.core);
            result
        }
    }

}

#[test]
fn context() {
    let mut buf = Context::init();
    buf.update(&[65u8]);
    let id = buf.final();
    assert!(id.to_hex() == "6dcd4ce23d88e2ee9568ba546c007c63d9131c1b".to_string());
}

#[cfg(test)]
impl Oid {
    // When testing, it is useful to produce a tweaked Oid that is
    // slightly larger or smaller than the given one.
    fn tweak(&self, adjust: int, stop: u8) -> Oid {
        let mut result = (*self).clone();
        let mut pos = 19;
        loop {
            let tmp = (result.bytes[pos] as int + adjust) as u8;
            result.bytes[pos] = tmp;
            if tmp == stop {
                if pos == 0 {
                    break;
                }
                pos -= 1;
            } else {
                break;
            }
        }
        // println!("tweak: {} -> {} (adj={}, stop={})",
        //     self.to_hex(), result.to_hex(), adjust, stop);
        result
    }

    pub fn inc(&self) -> Oid {
        self.tweak(1, 0)
    }

    pub fn dec(&self) -> Oid {
        self.tweak(-1, 255)
    }
}

#[cfg(test)]
fn tweaker(input: &str, expect: &str, amount: int) {
    let mut work = Oid::from_hex(input).unwrap();
    let mut tmp = amount;
    while tmp > 0 {
        work = work.inc();
        tmp -= 1;
    }
    while tmp < 0 {
        work = work.dec();
        tmp += 1;
    }
    if Oid::from_hex(expect).unwrap() != work {
        fail!("Expecting {}, but got {}, amount {}",
              expect, work.to_hex(), amount);
    }
    // assert!(Oid::from_hex(expect).unwrap() == work);
}

#[test]
fn test_tweak() {
    let a = Oid::from_data(kind!("blob"), "1".as_bytes());
    let b = a.inc();
    assert!(a != b);
    let c = b.dec();
    assert!(a == c);

    tweaker("0000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000001",
            1);
    tweaker("0000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000100",
            256);
    tweaker("00000000000000000000000000000000ffffffff",
            "0000000000000000000000000000000100000000",
            1);
    tweaker("ffffffffffffffffffffffffffffffffffffffff",
            "0000000000000000000000000000000000000000",
            1);

    tweaker("ffffffffffffffffffffffffffffffffffffffff",
            "fffffffffffffffffffffffffffffffffffffffe",
            -1);
    tweaker("ffffffffffffffffffffffffffffffffffffffff",
            "fffffffffffffffffffffffffffffffffffffeff",
            -256);
    tweaker("ffffffffffffffffffffffffffffffff00000000",
            "fffffffffffffffffffffffffffffffeffffffff",
            -1);
    tweaker("0000000000000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffffffffffff",
            -1);
}

impl fmt::Show for Oid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.bytes[0])
    }
}

impl Oid {
    pub fn to_hex(&self) -> String {
        let mut result = String::new();
        for i in range(0u, 20) {
            result.push_str(format!("{:02x}", self.bytes[i]).as_slice());
        }
        result
    }

    pub fn from_hex(text: &str) -> Option<Oid> {
        if text.len() != 40 {
            return None
        }
        let bytes = text.as_bytes();
        let mut result: Oid = unsafe { mem::uninitialized() };
        for (i, ch) in bytes.chunks(2).enumerate() {
            match ::std::u8::parse_bytes(ch, 16) {
                None => return None,
                Some(b) => result.bytes[i] = b,
            }
        }
        Some(result)
    }

    pub fn from_data(kind: Kind, data: &[u8]) -> Oid {
        let mut ctx = Context::init();
        kind.to_bytes(|v| ctx.update(v));
        ctx.update(data);
        ctx.final()
    }
}

#[test]
fn data_hashes() {
    assert!(Oid::from_data(kind!("blob"), "Simple".as_bytes()) ==
            Oid::from_hex("9d91380b823559dd2a4ee5bce3fcc697c56ba3f8").unwrap());
    assert!(Oid::from_data(kind!("zot "), "".as_bytes()) ==
            Oid::from_hex("bfc24abdb6cec5eae7d3dd84686117902ad2b562").unwrap());
}

#[test]
fn invalid_oid() {
    assert!(Oid::from_hex("9d91380b823559dd2a4ee5bce3fcc697c56ba3") == None);
}
