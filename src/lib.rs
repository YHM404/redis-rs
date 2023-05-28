#![feature(int_roundings)]

pub mod protobuf;
pub mod service;
pub mod store;
pub mod utils;

pub const REDIS_NODE_ID_PREFIX: &str = "REDIS_NODE";
pub const PROXY_NODE_ID_PREFIX: &str = "PROXY_NODE";

pub const SLOTS_MAPPING: &str = "SLOTS_MAPPING";

pub const SLOTS_LENGTH: usize = 1024;
