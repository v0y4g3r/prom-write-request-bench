#![feature(allocator_api)]

pub mod bytes;
pub mod prom_write_request;
pub mod repeated_field;

use bumpalo::Bump;
