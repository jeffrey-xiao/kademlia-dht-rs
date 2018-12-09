use crate::key::Key;
use crate::node::node_data::NodeData;
use crate::{BUCKET_REFRESH_INTERVAL, REPLICATION_PARAM, ROUTING_TABLE_SIZE};
use std::sync::Arc;
use std::{cmp, mem};
use time::{Duration, SteadyTime};

/// A k-bucket in a node's routing table that has a maximum capacity of `REPLICATION_PARAM`.
///
/// The nodes in the k-bucket are sorted by the time of the most recent communication with those
/// which have been most recently communicated at the end of the list.
#[derive(Clone, Debug)]
struct RoutingBucket {
    nodes: Vec<NodeData>,
    last_update_time: SteadyTime,
}

impl RoutingBucket {
    /// Constructs a new, empty `RoutingBucket`.
    fn new() -> Self {
        RoutingBucket {
            nodes: Vec::new(),
            last_update_time: SteadyTime::now(),
        }
    }

    /// Upserts a node in the routing bucket. If the node already exists in the routing bucket, the
    /// node will be moved to the end of the list. If the routing bucket is at capacity, it will
    /// remove the node least recently communicated with to create room for the new node.
    /// Additionally, `last_update_time` is also updated.
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

    /// Returns `true` if the `node_data` exists in the routing bucket.
    fn contains(&self, node_data: &NodeData) -> bool {
        self.nodes.iter().any(|data| data == node_data)
    }

    /// Splits `self` by a particular index and returns the closer bucket.
    fn split(&mut self, key: &Key, index: usize) -> RoutingBucket {
        let (old_bucket, new_bucket) = self
            .nodes
            .drain(..)
            .partition(|node| node.id.xor(key).leading_zeros() == index);
        mem::replace(&mut self.nodes, old_bucket);
        RoutingBucket {
            nodes: new_bucket,
            last_update_time: self.last_update_time,
        }
    }

    /// Returns a slice of the nodes contained by the routing bucket.
    fn get_nodes(&self) -> &[NodeData] {
        self.nodes.as_slice()
    }

    /// Removes the least recently seen node from the routing bucket.
    fn remove_lrs(&mut self) -> Option<NodeData> {
        if self.size() == 0 {
            None
        } else {
            Some(self.nodes.remove(0))
        }
    }

    /// Removes `node_data` from the routing bucket.
    pub fn remove_node(&mut self, node_data: &NodeData) -> Option<NodeData> {
        if let Some(index) = self.nodes.iter().position(|data| data == node_data) {
            Some(self.nodes.remove(index))
        } else {
            None
        }
    }

    /// Returns `true` if the routing bucket has not been recently updated.
    ///
    /// A bucket is stale if it has not been updated in `BUCKET_REFRESH_INTERVAL` seconds.
    pub fn is_stale(&self) -> bool {
        let time_diff = SteadyTime::now() - self.last_update_time;
        time_diff > Duration::seconds(BUCKET_REFRESH_INTERVAL as i64)
    }

    /// Returns the number of nodes in the routing bucket.
    pub fn size(&self) -> usize {
        self.nodes.len()
    }
}

/// A node's routing table tree.
///
/// `RoutingTable` is implemented using a growable vector of `RoutingBucket`. The relaxation of
/// k-bucket splitting proposed in Section 4.2 is not implemented.
#[derive(Clone, Debug)]
pub struct RoutingTable {
    buckets: Vec<RoutingBucket>,
    node_data: Arc<NodeData>,
}

impl RoutingTable {
    /// Constructs a new, empty `RoutingTable`.
    pub fn new(node_data: Arc<NodeData>) -> Self {
        let mut buckets = Vec::new();
        buckets.push(RoutingBucket::new());
        RoutingTable { buckets, node_data }
    }

    /// Upserts a node into the routing table. It will continue to split the routing table until the
    /// routing table is full or until the node can be upserted.
    pub fn update_node(&mut self, node_data: NodeData) -> bool {
        let distance = self.node_data.id.xor(&node_data.id).leading_zeros();
        let mut target_bucket = cmp::min(distance, self.buckets.len() - 1);

        if self.buckets[target_bucket].contains(&node_data) {
            self.buckets[target_bucket].update_node(node_data);
            return true;
        }

        loop {
            // bucket is not full
            if self.buckets[target_bucket].size() < REPLICATION_PARAM {
                self.buckets[target_bucket].update_node(node_data);
                return true;
            }

            let is_last_bucket = target_bucket == self.buckets.len() - 1;
            let is_full = self.buckets.len() == ROUTING_TABLE_SIZE;

            // bucket cannot be split
            if !is_last_bucket || is_full {
                return false;
            }

            // split bucket
            let new_bucket = self.buckets[target_bucket].split(&self.node_data.id, target_bucket);
            self.buckets.push(new_bucket);

            target_bucket = cmp::min(distance, self.buckets.len() - 1);
        }
    }

    /// Returns the closest `count` nodes to `key`.
    pub fn get_closest_nodes(&self, key: &Key, count: usize) -> Vec<NodeData> {
        let index = cmp::min(
            self.node_data.id.xor(key).leading_zeros(),
            self.buckets.len() - 1,
        );
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

    /// Removes the least recently seen node from a particular routing bucket in the routing table.
    pub fn remove_lrs(&mut self, key: &Key) -> Option<NodeData> {
        let index = cmp::min(
            self.node_data.id.xor(key).leading_zeros(),
            self.buckets.len() - 1,
        );
        self.buckets[index].remove_lrs()
    }

    /// Removes `node_data` from the routing table.
    pub fn remove_node(&mut self, node_data: &NodeData) {
        let index = cmp::min(
            self.node_data.id.xor(&node_data.id).leading_zeros(),
            self.buckets.len() - 1,
        );
        self.buckets[index].remove_node(node_data);
    }

    /// Returns a list of all the stale routing buckets in the routing table.
    pub fn get_stale_indexes(&self) -> Vec<usize> {
        let mut ret = Vec::new();
        for (i, bucket) in self.buckets.iter().enumerate() {
            if bucket.is_stale() {
                ret.push(i);
            }
        }
        ret
    }

    /// Returns the number of routing buckets in the routing table.
    pub fn size(&self) -> usize {
        self.buckets.len()
    }
}
