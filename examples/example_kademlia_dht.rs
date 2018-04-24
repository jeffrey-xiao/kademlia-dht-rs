extern crate kademlia_dht;
extern crate sha3;

use kademlia_dht::{Key, Node};
use sha3::{Digest, Sha3_256};
use std::thread;
use std::time::Duration;

fn clone_into_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Clone,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
    a
}

fn get_key(key: &str) -> Key {
    let mut hasher = Sha3_256::default();
    hasher.input(key.as_bytes());
    Key(clone_into_array(hasher.result().as_slice()))
}

fn main() {
    let mut node = Node::new("localhost", "8080", None);

    let key = get_key("Hello");
    let value = "World";

    node.insert(key, value);

    // inserting is asynchronous, so sleep for a second
    thread::sleep(Duration::from_millis(1000));

    assert_eq!(node.get(&key).unwrap(), value);
}
