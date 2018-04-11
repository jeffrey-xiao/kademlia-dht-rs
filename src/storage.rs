use std::collections::{BTreeMap, HashMap};
use std::vec::Vec;
use std::mem;
use time::{Duration, SteadyTime};

use key::Key;
use KEY_EXPIRATION;

/// A simple storage container that removes stale items.
///
/// `Storage` will remove a item if it is older than `KEY_EXPIRATION` seconds.
#[derive(Default)]
pub struct Storage {
    items: HashMap<Key, String>,
    publish_times: BTreeMap<SteadyTime, Vec<Key>>,
}

impl Storage {
    /// Constructs a new, empty `Storage`.
    pub fn new() -> Self {
        Storage {
            items: HashMap::new(),
            publish_times: BTreeMap::new(),
        }
    }

    /// Removes all items that are older than `KEY_EXPIRATION` seconds.
    fn remove_expired(&mut self) {
        let expiration_cutoff = SteadyTime::now() - Duration::seconds(KEY_EXPIRATION as i64);
        let mut expired_times_map = self.publish_times.split_off(&expiration_cutoff);
        mem::swap(&mut self.publish_times, &mut expired_times_map);

        for key in expired_times_map.into_iter().flat_map(|entry| entry.1.into_iter()) {
            info!("Removed {:?}", key);
            self.items.remove(&key);
        }
    }

    /// Inserts an item into `Storage`.
    pub fn insert(&mut self, key: Key, value: String) {
        self.remove_expired();
        let curr_time = SteadyTime::now();

        self.items.insert(key, value);
        self.publish_times.entry(curr_time).or_insert_with(Vec::new).push(key);
    }

    /// Returns the value associated with `key`. Returns `None` if such a key does not exist in
    /// `Storage`.
    pub fn get(&mut self, key: &Key) -> Option<&String> {
        self.remove_expired();
        self.items.get(key)
    }
}
