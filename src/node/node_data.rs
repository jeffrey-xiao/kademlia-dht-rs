use std::cmp::Ordering;

use key::Key;
use std::fmt::{Debug, Formatter, Result};

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct NodeData {
    pub addr: String,
    pub id: Key,
}

impl Debug for NodeData {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{} - {:?}", self.addr, self.id)
    }
}

#[derive(Eq, Clone, Debug)]
pub struct NodeDataDistancePair(pub NodeData, pub Key);

impl PartialEq for NodeDataDistancePair {
    fn eq(&self, other: &NodeDataDistancePair) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for NodeDataDistancePair {
    fn partial_cmp(&self, other: &NodeDataDistancePair) -> Option<Ordering> {
        Some(other.1.cmp(&self.1))
    }
}

impl Ord for NodeDataDistancePair {
    fn cmp(&self, other: &NodeDataDistancePair) -> Ordering {
        other.1.cmp(&self.1)
    }
}

#[cfg(test)]
mod tests {}
