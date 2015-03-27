// Tmp to build the test.

// #![crate_name = "libpool"]
// #![crate_type = "rlib"]
// #![crate_type = "dylib"]

#![plugin(fourcc)]
#![feature(plugin)]

// Needed for raw manipulation in kind.  Maybe shouldn't use?
#![feature(core)]

// Collections aren't considered stable yet.
#![feature(collections)]

// Needed for libc
#![feature(libc)]

// Needed to call .exists() on a Path.
#![feature(path_ext)]

// Until type ascription is implemeneted, we need trivial casts.
#![allow(trivial_casts)]

#![cfg_attr(test, feature(test))]

/// Rust dump

pub use error::{Error, Result};

extern crate core;
extern crate libc;

#[cfg(test)]
extern crate tempdir;

extern crate flate2;
// // extern crate collections;
// extern crate flate;
extern crate uuid;
// extern crate sqlite3;
extern crate rusqlite;
extern crate rustc_serialize;

#[macro_use]
extern crate log;

#[cfg(test)]
extern crate test;

#[cfg(test)]
extern crate rand;

pub mod kind;

#[cfg(test)]
mod testutil;

// #[cfg(test)]
pub mod pdump;

pub mod oid;
pub mod chunk;
pub mod pool;
pub mod nodes;

mod error;
mod zlib;
