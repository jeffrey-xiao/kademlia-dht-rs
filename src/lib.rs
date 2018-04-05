#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate bincode;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate time;

pub mod protocol;
pub mod node;
pub mod key;
pub mod storage;
pub mod routing;

pub use self::node::Node;

const KEY_LENGTH: usize = 32;
const MESSAGE_LENGTH: usize = 8196;
const ROUTING_TABLE_SIZE: usize = KEY_LENGTH * 8;

const REPLICATION_PARAM: usize = 20;
const CONCURRENCY_PARAM: usize = 3;

// Request timeout time in milliseconds
const REQUEST_TIMEOUT: u64 = 5000;

// Key-value pair expiration time in seconds
const KEY_EXPIRATION: u64 = 3600;

// Bucket refresh interval in seconds
const BUCKET_REFRESH_INTERVAL: u64 = 3600;
