// Tmp to build the test.

// Suppress error about compile time plugins.
#![feature(phase)]
#![feature(macro_rules)]

extern crate core;
extern crate libc;
extern crate collections;
extern crate flate;

#[phase(plugin)]
extern crate fourcc;

#[phase(plugin, link)]
extern crate log;

#[cfg(test)]
extern crate test;

#[cfg(test)]
mod testutil;

#[cfg(test)]
mod pdump;

pub mod kind;
pub mod oid;
pub mod chunk;
