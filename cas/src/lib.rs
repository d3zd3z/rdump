// Copyright 2015 David Brown
// MIT License

// #![allow(dead_code)]

extern crate byteorder;
extern crate libc;
extern crate rustc_serialize;
extern crate flate2;
extern crate rusqlite;
extern crate uuid;

#[cfg(test)]
extern crate rand;

/*
#[cfg(test)]
extern crate test;
*/

#[cfg(test)]
extern crate tempdir;

pub use error::Error;

use std::result;

pub type Result<T> = result::Result<T, Error>;

pub mod error;
pub mod kind;
pub mod oid;
pub mod chunk;
pub mod pdump;
pub mod pool;

mod zlib;

#[cfg(test)]
mod testutil;
