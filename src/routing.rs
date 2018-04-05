use std::sync::Arc;

use std::{cmp, mem};
use time::{Duration, SteadyTime};

use {BUCKET_REFRESH_INTERVAL, REPLICATION_PARAM, ROUTING_TABLE_SIZE};
use node::node_data::NodeData;
use key::Key;

#[derive(Clone, Debug)]
struct RoutingBucket {
    nodes: Vec<NodeData>,
    last_update_time: SteadyTime,
}

impl RoutingBucket {
    fn new() -> Self {
        RoutingBucket {
            nodes: Vec::new(),
            last_update_time: SteadyTime::now(),
        }
    }

    fn update_node(&mut self, node_data: NodeData) {
        self.last_update_time = SteadyTime::now();
        if let Some(index) = self.nodes.iter().position(|data| *data == node_data) {
            self.nodes.remove(index);
        }
        self.nodes.push(node_data);
        if self.nodes.len() > REPLICATION_PARAM {
            self.nodes.remove(0);
        }
    }

    fn contains(&self, node_data: &NodeData) -> bool {
        self.nodes.iter().any(|data| data == node_data)
    }

    fn split(&mut self, key: &Key, index: usize) -> RoutingBucket {
        let (old_bucket, new_bucket) = self.nodes
            .drain(..)
            .partition(|node| node.id.xor(key).leading_zeros() == index);
        mem::replace(&mut self.nodes, old_bucket);
        RoutingBucket {
            nodes: new_bucket,
            last_update_time: self.last_update_time,
        }
    }

    fn get_nodes(&self) -> &[NodeData] {
        self.nodes.as_slice()
    }

    fn remove_lrs(&mut self) -> Option<NodeData> {
        if self.size() == 0 {
            None
        } else {
            Some(self.nodes.remove(0))
        }
    }

    pub fn remove_node(&mut self, node_data: &NodeData) -> Option<NodeData> {
        if let Some(index) = self.nodes.iter().position(|data| data == node_data) {
            Some(self.nodes.remove(index))
        } else {
            None
        }
    }

    pub fn is_stale(&self) -> bool {
        SteadyTime::now() - self.last_update_time > Duration::seconds(BUCKET_REFRESH_INTERVAL as i64)
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }
}

// An implementation of the routing table tree using a vector of buckets
#[derive(Clone, Debug)]
pub struct RoutingTable {
    buckets: Vec<RoutingBucket>,
    node_data: Arc<NodeData>,
}

impl RoutingTable {
    pub fn new(node_data: Arc<NodeData>) -> Self {
        let mut buckets = Vec::new();
        buckets.push(RoutingBucket::new());
        RoutingTable {
            buckets: buckets,
            node_data: node_data,
        }
    }

    pub fn update_node(&mut self, node_data: NodeData) -> bool {
        let distance = self.node_data.id.xor(&node_data.id).leading_zeros();
        let mut target_bucket = cmp::min(distance, self.buckets.len() - 1);
        if self.buckets[target_bucket].contains(&node_data) {
            self.buckets[target_bucket].update_node(node_data);
            return true;
        }
        loop {
            // bucket is full
            if self.buckets[target_bucket].size() == REPLICATION_PARAM {
                // split bucket
                if target_bucket == self.buckets.len() - 1 && self.buckets.len() < ROUTING_TABLE_SIZE {
                    let new_bucket = self.buckets[target_bucket].split(&self.node_data.id, target_bucket);
                    self.buckets.push(new_bucket);
                }
                // bucket cannot be split
                else {
                    return false;
                }
            }
            // add into bucket
            else {
                self.buckets[target_bucket].update_node(node_data);
                return true;
            }
            target_bucket = cmp::min(distance, self.buckets.len() - 1);
        }
    }

    pub fn get_closest_nodes(&self, key: &Key, count: usize) -> Vec<NodeData> {
        let index = cmp::min(self.node_data.id.xor(key).leading_zeros(), self.buckets.len() - 1);
        let mut ret = Vec::new();

        // the closest keys are guaranteed to be in bucket which the key would reside
        ret.extend_from_slice(self.buckets[index].get_nodes());

        if ret.len() < count {
            // the distance between target key and keys is not necessarily monotonic
            // in range (key.leading_zeros(), self.buckets.len()], so we must iterate
            for i in (index + 1)..self.buckets.len() {
                ret.extend_from_slice(self.buckets[i].get_nodes());
            }
        }

        if ret.len() < count {
            // the distance between target key and keys in [0, key.leading_zeros())
            // is monotonicly decreasing by bucket
            for i in (0..index).rev() {
                ret.extend_from_slice(self.buckets[i].get_nodes());
                if ret.len() >= count {
                    break;
                }
            }
        }

        ret.sort_by_key(|node| node.id.xor(key));
        ret.truncate(count);
        ret
    }

    pub fn remove_lrs(&mut self, key: &Key) -> Option<NodeData> {
        let index = cmp::min(self.node_data.id.xor(key).leading_zeros(), self.buckets.len() - 1);
        self.buckets[index].remove_lrs()
    }

    pub fn remove_node(&mut self, node_data: &NodeData) {
        let index = cmp::min(self.node_data.id.xor(&node_data.id).leading_zeros(), self.buckets.len() - 1);
        self.buckets[index].remove_node(node_data);
    }

    pub fn get_stale_indexes(&self)-> Vec<usize> {
        let mut ret = Vec::new();
        for (i, bucket) in self.buckets.iter().enumerate() {
            if bucket.is_stale() {
                ret.push(i);
            }
        }
        ret
    }

    pub fn size(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {}
