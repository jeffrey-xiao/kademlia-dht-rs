use std::collections::{BTreeMap, HashMap};
use std::vec::Vec;
use std::mem;
use time::{Duration, SteadyTime};

use key::Key;
use KEY_EXPIRATION;

#[derive(Default)]
pub struct Storage {
    data: HashMap<Key, String>,
    publish_times: BTreeMap<SteadyTime, Vec<Key>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: HashMap::new(),
            publish_times: BTreeMap::new(),
        }
    }

    fn remove_expired(&mut self) {
        let expiration_cutoff = SteadyTime::now() - Duration::seconds(KEY_EXPIRATION as i64);
        let mut expired_times_map = self.publish_times.split_off(&expiration_cutoff);
        mem::swap(&mut self.publish_times, &mut expired_times_map);

        for key in expired_times_map.into_iter().flat_map(|entry| entry.1.into_iter()) {
            info!("Removed {:?}", key);
            self.data.remove(&key);
        }
    }

    pub fn insert(&mut self, key: Key, value: String) {
        self.remove_expired();
        let curr_time = SteadyTime::now();

        self.data.insert(key, value);
        self.publish_times.entry(curr_time).or_insert_with(Vec::new).push(key);
    }

    pub fn get(&mut self, key: &Key) -> Option<&String> {
        self.remove_expired();
        self.data.get(key)
    }
}
