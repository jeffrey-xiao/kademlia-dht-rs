//! # kademlia-dht-rs
//!
//! [![kademlia-dht](http://meritbadge.herokuapp.com/kademlia-dht)](https://crates.io/crates/kademlia-dht)
//! [![Documentation](https://docs.rs/kademlia-dht/badge.svg)](https://docs.rs/kademlia-dht)
//! [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
//! [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
//! [![Build Status](https://travis-ci.org/jeffrey-xiao/kademlia-dht-rs.svg?branch=master)](https://travis-ci.org/jeffrey-xiao/kademlia-dht-rs)
//! [![codecov](https://codecov.io/gh/jeffrey-xiao/kademlia-dht-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/jeffrey-xiao/kademlia-dht-rs)
//!
//! A flexible implementation of the Kademlia distributed hash table. This library crate was mainly
//! created to better understand the Rust concurrency primitives. This implementation is fairly
//! close to the spec described in the original Kademlia paper with the exception of a few design
//! considerations.
//!
//! ## Examples
//!
//! ```rust
//! extern crate kademlia_dht;
//! extern crate sha3;
//!
//! use kademlia_dht::{Key, Node};
//! use sha3::{Digest, Sha3_256};
//! use std::thread;
//! use std::time::Duration;
//!
//! fn clone_into_array<A, T>(slice: &[T]) -> A
//! where
//!     A: Sized + Default + AsMut<[T]>,
//!     T: Clone,
//! {
//!     let mut a = Default::default();
//!     <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
//!     a
//! }
//!
//! fn get_key(key: &str) -> Key {
//!     let mut hasher = Sha3_256::default();
//!     hasher.input(key.as_bytes());
//!     Key(clone_into_array(hasher.result().as_slice()))
//! }
//!
//! fn main() {
//!     let mut node = Node::new("localhost", "8080", None);
//!
//!     let key = get_key("Hello");
//!     let value = "World";
//!
//!     node.insert(key, value);
//!
//!     // inserting is asynchronous, so sleep for a second
//!     thread::sleep(Duration::from_millis(1000));
//!
//!     assert_eq!(node.get(&key).unwrap(), value);
//! }
//! ```
//!
//! ## Usage
//!
//! Add this to your `Cargo.toml`:
//! ```toml
//! [dependencies]
//! kademlia-dht = "*"
//! ```
//! and this to your crate root if you are using Rust 2015:
//! ```rust
//! extern crate kademlia_dht;
//! ```
//!
//! ## Design Considerations
//!
//!  - Many of the paper's original optimizations were not implemented due to their complexity for
//!    arguably little gain.
//!  - Each node's routing table uses a growable vector to represent the binary tree of k-buckets.
//!    The vector grows as the k-bucket closest to the node's ID exceeds capacity. The relaxation of
//!    k-bucket splitting proposed in Section 4.2 is not implemented.
//!  - Caching and key republishing described in Section 2.5 is not implemented to simplify the
//!    number of moving parts and active threads. It is up to the user of the library to ensure that
//!    keys are being republished.
//!  - The recursive lookup of nodes uses strict parallelism to tightly bound the number of active
//!    RPCs rather than the loose parallelism implied by the paper.
//!  - Each key is 256 bits as opposed to 160 bits so that consumers can use SHA-3 instead of SHA-1.
//!
//! ## Changelog
//!
//! See [CHANGELOG](CHANGELOG.md) for more details.
//!
//! ## References
//!
//!  - [Kademlia: A Peer-to-Peer Information System Based on the XOR
//!    Metric](https://dl.acm.org/citation.cfm?id=687801)
//!  > Maymounkov, Petar, and David Mazières. 2002. “Kademlia: A Peer-to-Peer Information System Based on the Xor Metric.” In *Revised Papers from the First International Workshop on Peer-to-Peer Systems*, 53–65. IPTPS ’01. London, UK, UK: Springer-Verlag. <http://dl.acm.org/citation.cfm?id=646334.687801>.
//!
//! ## License
//!
//! `kademlia-dht-rs` is dual-licensed under the terms of either the MIT License or the Apache
//! License (Version 2.0).
//!
//! See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT) for more details.

#![warn(missing_docs)]

mod key;
mod node;
mod protocol;
mod routing;
mod storage;

pub use self::key::Key;
pub use self::node::node_data::NodeData;
pub use self::node::Node;

/// The number of bytes in a key.
const KEY_LENGTH: usize = 32;

/// The maximum length of the message in bytes.
const MESSAGE_LENGTH: usize = 8196;

/// The maximum number of k-buckets in the routing table.
const ROUTING_TABLE_SIZE: usize = KEY_LENGTH * 8;

/// The maximum number of entries in a k-bucket.
const REPLICATION_PARAM: usize = 20;

/// The maximum number of active RPCs during `lookup_nodes`.
const CONCURRENCY_PARAM: usize = 3;

/// Request timeout time in milliseconds
const REQUEST_TIMEOUT: u64 = 5000;

/// Key-value pair expiration time in seconds
const KEY_EXPIRATION: u64 = 3600;

/// Bucket refresh interval in seconds
const BUCKET_REFRESH_INTERVAL: u64 = 3600;
