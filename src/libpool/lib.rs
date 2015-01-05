// Tmp to build the test.

#![crate_name = "libpool"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]

// Suppress error about compile time plugins.
#![feature(phase)]
#![feature(macro_rules)]

// Needed until https://github.com/rust-lang/rust/issues/13853 and/or
// https://github.com/rust-lang/rust/issues/14889 are fixed.
// #![feature(unsafe_destructor)]

/// Rust dump

extern crate core;
extern crate libc;
// extern crate collections;
extern crate flate;
// extern crate uuid;
// extern crate sqlite3;
extern crate "rustc-serialize" as rustc_serialize;

#[phase(plugin)]
extern crate fourcc;

#[phase(plugin, link)]
extern crate log;

#[cfg(test)]
extern crate test;

#[cfg(test)]
mod testutil;

#[cfg(test)]
pub mod pdump;

pub mod kind;
pub mod oid;
pub mod chunk;
// pub mod pool;
