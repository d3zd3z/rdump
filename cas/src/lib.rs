// Copyright 2015 David Brown
// MIT License

// #![allow(dead_code)]

extern crate byteorder;
extern crate libc;
extern crate rustc_serialize;
extern crate flate2;
extern crate rusqlite;
extern crate uuid;

// #[cfg(test)]
extern crate rand;

#[cfg(test)]
extern crate tempdir;

pub use error::Error;
pub use kind::Kind;
pub use oid::Oid;
pub use chunk::Chunk;
pub use chunk::Data;

use std::result;

pub type Result<T> = result::Result<T, Error>;

mod error;
mod kind;
mod oid;
pub mod chunk;
pub mod pdump;
pub mod pool;

mod zlib;

// #[cfg(test)]
pub mod testutil;
