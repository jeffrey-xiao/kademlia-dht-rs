#[macro_use]
extern crate log;
extern crate kademlia_dht;
extern crate simplelog;
extern crate sha3;

use simplelog::{CombinedLogger, TermLogger, Level, LevelFilter, Config};
use std::io;
use std::collections::HashMap;
use std::convert::AsMut;
use sha3::{Digest, Sha3_256};

use kademlia_dht::{Key, Node};

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
    let logger_config = Config {
        time: Some(Level::Error),
        level: Some(Level::Error),
        target: None,
        location: None,
        time_format: None,
    };
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info, logger_config).unwrap(),
        ],
    ).unwrap();

    let mut node_map = HashMap::new();
    let mut id = 0;
    for i in 0..50 {
        if i == 0 {
            let n = Node::new(&"localhost".to_string(), &(8900 + i).to_string(), None);
            node_map.insert(id, n.clone());
        } else {
            let n = Node::new(
                &"localhost".to_string(),
                &(8900 + i).to_string(),
                Some(node_map[&0].node_data()),
            );
            node_map.insert(id, n.clone());
        }
        id += 1;
    }

    for i in (1..50).filter(|num| num % 10 == 0) {
        node_map[&i].kill();
    }

    let input = io::stdin();

    loop {
        let mut buffer = String::new();
        println!("Ready for input!");
        if input.read_line(&mut buffer).is_err() {
            break;
        }
        let args: Vec<&str> = buffer.trim_right().split(' ').collect();
        match args[0] {
            "new" => {
                let index: u32 = args[1].parse().unwrap();
                let node = Node::new(
                    &"localhost".to_string(),
                    &(8900 + id).to_string(),
                    Some(node_map[&index].node_data()),
                );
                node_map.insert(id, node);
                id += 1;
            },
            "insert" => {
                let index: u32 = args[1].parse().unwrap();
                let key = get_key(args[2]);
                let value = args[3];
                node_map.get_mut(&index).unwrap().insert(key, value);
            },
            "get" => {
                let index: u32 = args[1].parse().unwrap();
                let key = get_key(args[2]);
                info!("{:?}", node_map.get_mut(&index).unwrap().get(&key));
            },
            _ => {},
        }
    }
}
