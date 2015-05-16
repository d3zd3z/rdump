// Object ID.

//! Object IDs.
//!
//! Every object in the pool is identified by an object-id (OID) which is
//! the SHA-1 hash of the `Kind` followed by the payload itself.

use std::mem;
// use std::slice::bytes;
use kind::Kind;

use rustc_serialize::hex::{ToHex, FromHex};

// TODO: Derive our own Debug and Hash.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Hash)]
pub struct Oid {
    pub bytes: [u8; 20],
}

// Simple binding to the crypto library from OpenSSL.
mod openssl {
    use libc::{c_int, c_uint, c_uchar, c_void, size_t, uint32_t};
    #[cfg(test)] use std::mem;

    // Despite the type name in the SSL header, these are expected to all
    // be 32-bit values.
    #[repr(C)]
    pub struct ShaCtx {
        _h0: uint32_t,
        _h1: uint32_t,
        _h2: uint32_t,
        _h3: uint32_t,
        _h4: uint32_t,
        _nl: uint32_t,
        _nh: uint32_t,
        _data: [uint32_t; 16],
        _num: c_uint,
    }

    #[link(name = "crypto")]
    extern {
        pub fn SHA1_Init(c: *mut ShaCtx) -> c_int;
        pub fn SHA1_Update(c: *mut ShaCtx, data: *const c_void, len: size_t) -> c_int;
        pub fn SHA1_Final(md: *mut c_uchar, c: *mut ShaCtx) -> c_int;
    }

    #[test]
    fn context_size() {
        assert_eq!(mem::size_of::<ShaCtx>(), 96);
    }
}

struct Context {
    core: openssl::ShaCtx,
}

impl Context {
    fn new() -> Context {
        unsafe {
            let mut result: Context = mem::uninitialized();
            openssl::SHA1_Init(&mut result.core);
            result
        }
    }

    fn update(&mut self, data: &[u8]) {
        unsafe {
            openssl::SHA1_Update(&mut self.core,
                                 data.as_ptr() as *const ::libc::c_void,
                                 data.len() as ::libc::size_t);
        }
    }

    fn finish(&mut self) -> Oid {
        unsafe {
            let mut result: Oid = mem::uninitialized();
            openssl::SHA1_Final(&mut result.bytes[0], &mut self.core);
            result
        }
    }
}

#[test]
fn test_context() {
    let mut buf = Context::new();
    buf.update(&[65u8]);
    let id = buf.finish();
    assert_eq!(id.to_hex(), "6dcd4ce23d88e2ee9568ba546c007c63d9131c1b");
}

impl Oid {
    pub fn to_hex(&self) -> String {
        self.bytes[..].to_hex()
    }

    pub fn from_hex(text: &str) -> Option<Oid> {
        if text.len() != 40 {
            return None
        }

        text.from_hex().ok().map(|x| Oid::from_raw(&x[..]))
    }

    pub fn from_raw(bytes: &[u8]) -> Oid {
        if bytes.len() != 20 {
            panic!("OID is incorrect length");
        }

        let mut result: Oid = unsafe { mem::uninitialized() };
        // bytes::copy_memory(bytes, &mut result.bytes[..]);
        for i in 0..20 {
            result.bytes[i] = bytes[i];
        }
        result
    }

    pub fn from_data(kind: Kind, data: &[u8]) -> Oid {
        let mut ctx = Context::new();
        ctx.update(&kind.bytes());
        ctx.update(data);
        ctx.finish()
    }
}

#[cfg(test)]
impl Oid {
    // When testing, it is useful to produce a tweaked Oid that is slightly
    // larger or smaller than the given one.
    fn tweak(&self, adjust: i16, stop: u8) -> Oid {
        let mut result = (*self).clone();
        let mut pos = 19;
        loop {
            let tmp = (result.bytes[pos] as i16 + adjust) as u8;
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
fn tweaker(input: &str, expect: &str, amount: i16) {
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
        panic!("Expecting {}, but got {}, amount {}",
               expect, work.to_hex(), amount);
    }
}

#[test]
fn test_tweak() {
    let a = Oid::from_data(Kind::new("blob").unwrap(), "1".as_bytes());
    let b = a.inc();
    assert!(a != b);
    let c = b.dec();
    assert_eq!(a, c);

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
