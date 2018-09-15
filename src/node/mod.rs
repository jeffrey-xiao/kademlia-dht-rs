pub mod node_data;

use std::cmp;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use key::Key;
use node::node_data::{NodeData, NodeDataDistancePair};
use protocol::{Message, Protocol, Request, RequestPayload, Response, ResponsePayload};
use routing::RoutingTable;
use storage::Storage;
use {BUCKET_REFRESH_INTERVAL, CONCURRENCY_PARAM, KEY_LENGTH, REPLICATION_PARAM, REQUEST_TIMEOUT};

/// A node in the Kademlia DHT.
#[derive(Clone)]
pub struct Node {
    node_data: Arc<NodeData>,
    routing_table: Arc<Mutex<RoutingTable>>,
    storage: Arc<Mutex<Storage>>,
    pending_requests: Arc<Mutex<HashMap<Key, Sender<Response>>>>,
    protocol: Arc<Protocol>,
    is_active: Arc<AtomicBool>,
}

impl Node {
    /// Constructs a new `Node` on a specific ip and port, and bootstraps the node with an existing
    /// node if `bootstrap` is not `None`.
    pub fn new(ip: &str, port: &str, bootstrap: Option<NodeData>) -> Self {
        let addr = format!("{}:{}", ip, port);
        let socket = UdpSocket::bind(addr).expect("Error: could not bind to address.");
        let node_data = Arc::new(NodeData {
            addr: socket.local_addr().unwrap().to_string(),
            id: Key::rand(),
        });
        let mut routing_table = RoutingTable::new(Arc::clone(&node_data));
        let (message_tx, message_rx) = channel();
        let protocol = Protocol::new(socket, message_tx);

        // directly use update_node as update_routing_table is async
        if let Some(bootstrap_data) = bootstrap {
            routing_table.update_node(bootstrap_data);
        }

        let mut ret = Node {
            node_data,
            routing_table: Arc::new(Mutex::new(routing_table)),
            storage: Arc::new(Mutex::new(Storage::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            protocol: Arc::new(protocol),
            is_active: Arc::new(AtomicBool::new(true)),
        };

        ret.start_message_handler(message_rx);
        ret.start_bucket_refresher();
        ret.bootstrap_routing_table();
        ret
    }

    /// Starts a thread that listens to responses.
    fn start_message_handler(&self, rx: Receiver<Message>) {
        let mut node = self.clone();
        thread::spawn(move || {
            for request in rx.iter() {
                match request {
                    Message::Request(request) => node.handle_request(&request),
                    Message::Response(response) => node.handle_response(&response),
                    Message::Kill => {
                        node.is_active.store(false, Ordering::Release);
                        info!("{} - Killed message handler", node.node_data.addr);
                        break;
                    },
                }
            }
        });
    }

    /// Starts a thread that refreshes stale routing buckets.
    fn start_bucket_refresher(&self) {
        let mut node = self.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(BUCKET_REFRESH_INTERVAL));
            while node.is_active.load(Ordering::Acquire) {
                let stale_indexes = {
                    let routing_table = match node.routing_table.lock() {
                        Ok(routing_table) => routing_table,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    routing_table.get_stale_indexes()
                };

                for index in stale_indexes {
                    node.lookup_nodes(&Key::rand_in_range(index), true);
                }
                thread::sleep(Duration::from_secs(BUCKET_REFRESH_INTERVAL));
            }
            warn!("{} - Killed bucket refresher", node.node_data.addr);
        });
    }

    /// Bootstraps the routing table using an existing node. The node first looks up its id to
    /// identify the closest nodes to it. Then it refreshes all routing buckets by looking up a
    /// random key in the buckets' range.
    fn bootstrap_routing_table(&mut self) {
        let target_key = self.node_data.id;
        self.lookup_nodes(&target_key, true);

        let bucket_size = { self.routing_table.lock().unwrap().size() };

        for i in 0..bucket_size {
            self.lookup_nodes(&Key::rand_in_range(i), true);
        }
    }

    /// Upserts the routing table. If the node cannot be inserted into the routing table, it
    /// removes and pings the least recently seen node. If the least recently seen node responds,
    /// it will be readded into the routing table, and the current node will be ignored.
    fn update_routing_table(&mut self, node_data: NodeData) {
        debug!("{} updating {}", self.node_data.addr, node_data.addr);
        let mut node = self.clone();
        thread::spawn(move || {
            let lrs_node_opt = {
                let mut routing_table = match node.routing_table.lock() {
                    Ok(routing_table) => routing_table,
                    Err(poisoned) => poisoned.into_inner(),
                };
                if !routing_table.update_node(node_data.clone()) {
                    routing_table.remove_lrs(&node_data.id)
                } else {
                    None
                }
            };

            // Ping the lrs node and move to front of bucket if active
            if let Some(lrs_node) = lrs_node_opt {
                node.rpc_ping(&lrs_node);
                let mut routing_table = match node.routing_table.lock() {
                    Ok(routing_table) => routing_table,
                    Err(poisoned) => poisoned.into_inner(),
                };
                routing_table.update_node(node_data);
            }
        });
    }

    /// Handles a request RPC.
    fn handle_request(&mut self, request: &Request) {
        info!(
            "{} - Receiving request from {} {:#?}",
            self.node_data.addr, request.sender.addr, request.payload,
        );
        self.clone().update_routing_table(request.sender.clone());
        let receiver = (*self.node_data).clone();
        let payload = match request.payload.clone() {
            RequestPayload::Ping => ResponsePayload::Pong,
            RequestPayload::Store(key, value) => {
                self.storage.lock().unwrap().insert(key, value);
                ResponsePayload::Pong
            },
            RequestPayload::FindNode(key) => {
                ResponsePayload::Nodes(
                    self.routing_table
                        .lock()
                        .unwrap()
                        .get_closest_nodes(&key, REPLICATION_PARAM),
                )
            },
            RequestPayload::FindValue(key) => {
                if let Some(value) = self.storage.lock().unwrap().get(&key) {
                    ResponsePayload::Value(value.clone())
                } else {
                    ResponsePayload::Nodes(
                        self.routing_table
                            .lock()
                            .unwrap()
                            .get_closest_nodes(&key, REPLICATION_PARAM),
                    )
                }
            },
        };

        self.protocol.send_message(
            &Message::Response(Response {
                request: request.clone(),
                receiver,
                payload,
            }),
            &request.sender,
        )
    }

    /// Handles a response RPC. If the id in the response does not match any outgoing request, then
    /// the response will be ignored.
    fn handle_response(&mut self, response: &Response) {
        self.clone().update_routing_table(response.receiver.clone());
        let pending_requests = self.pending_requests.lock().unwrap();
        let Response { ref request, .. } = response.clone();
        if let Some(sender) = pending_requests.get(&request.id) {
            info!(
                "{} - Receiving response from {} {:#?}",
                self.node_data.addr, response.receiver.addr, response.payload,
            );
            sender.send(response.clone()).unwrap();
        } else {
            warn!(
                "{} - Original request not found; irrelevant response or expired request.",
                self.node_data.addr
            );
        }
    }

    /// Sends a request RPC.
    fn send_request(&mut self, dest: &NodeData, payload: RequestPayload) -> Option<Response> {
        info!(
            "{} - Sending request to {} {:#?}",
            self.node_data.addr, dest.addr, payload
        );
        let (response_tx, response_rx) = channel();
        let mut pending_requests = self.pending_requests.lock().unwrap();
        let mut token = Key::rand();

        while pending_requests.contains_key(&token) {
            token = Key::rand();
        }
        pending_requests.insert(token, response_tx);
        drop(pending_requests);

        self.protocol.send_message(
            &Message::Request(Request {
                id: token,
                sender: (*self.node_data).clone(),
                payload,
            }),
            dest,
        );

        match response_rx.recv_timeout(Duration::from_millis(REQUEST_TIMEOUT)) {
            Ok(response) => {
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                Some(response)
            },
            Err(_) => {
                warn!(
                    "{} - Request to {} timed out after waiting for {} milliseconds",
                    self.node_data.addr, dest.addr, REQUEST_TIMEOUT
                );
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                let mut routing_table = self.routing_table.lock().unwrap();
                routing_table.remove_node(dest);
                None
            },
        }
    }

    /// Sends a `PING` RPC.
    fn rpc_ping(&mut self, dest: &NodeData) -> Option<Response> {
        self.send_request(dest, RequestPayload::Ping)
    }

    /// Sends a `STORE` RPC.
    fn rpc_store(&mut self, dest: &NodeData, key: Key, value: String) -> Option<Response> {
        self.send_request(dest, RequestPayload::Store(key, value))
    }

    /// Sends a `FIND_NODE` RPC.
    fn rpc_find_node(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindNode(*key))
    }

    /// Sends a `FIND_VALUE` RPC.
    fn rpc_find_value(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindValue(*key))
    }

    /// Spawns a thread that sends either a `FIND_NODE` or a `FIND_VALUE` RPC.
    fn spawn_find_rpc(
        mut self,
        dest: NodeData,
        key: Key,
        sender: Sender<Option<Response>>,
        find_node: bool,
    ) {
        thread::spawn(move || {
            let find_err = {
                if find_node {
                    sender.send(self.rpc_find_node(&dest, &key)).is_err()
                } else {
                    sender.send(self.rpc_find_value(&dest, &key)).is_err()
                }
            };

            if find_err {
                warn!("Receiver closed channel before rpc returned.");
            }
        });
    }

    /// Iteratively looks up nodes to determine the closest nodes to `key`. The search begins by
    /// selecting `CONCURRENCY_PARAM` nodes in the routing table and adding it to a shortlist. It
    /// then sends out either `FIND_NODE` or `FIND_VALUE` RPCs to `CONCURRENCY_PARAM` nodes not yet
    /// queried in the shortlist. The node will continue to fill its shortlist until it did not find
    /// a closer node for a round of RPCs or if runs out of nodes to query. Finally, it will query
    /// the remaining nodes in its shortlist until there are no remaining nodes or if it has found
    /// `REPLICATION_PARAM` active nodes.
    fn lookup_nodes(&mut self, key: &Key, find_node: bool) -> ResponsePayload {
        let routing_table = self.routing_table.lock().unwrap();
        let closest_nodes = routing_table.get_closest_nodes(key, CONCURRENCY_PARAM);
        drop(routing_table);

        let mut closest_distance = Key::new([255u8; KEY_LENGTH]);
        for node_data in &closest_nodes {
            closest_distance = cmp::min(closest_distance, key.xor(&node_data.id))
        }

        // initialize found nodes, queried nodes, and priority queue
        let mut found_nodes: HashSet<NodeData> = closest_nodes.clone().into_iter().collect();
        found_nodes.insert((*self.node_data).clone());
        let mut queried_nodes = HashSet::new();
        queried_nodes.insert((*self.node_data).clone());

        let mut queue: BinaryHeap<NodeDataDistancePair> = BinaryHeap::from(
            closest_nodes
                .into_iter()
                .map(|node_data| NodeDataDistancePair(node_data.clone(), node_data.id.xor(key)))
                .collect::<Vec<NodeDataDistancePair>>(),
        );

        let (tx, rx) = channel();

        let mut concurrent_thread_count = 0;

        // spawn initial find requests
        for _ in 0..CONCURRENCY_PARAM {
            if !queue.is_empty() {
                self.clone().spawn_find_rpc(
                    queue.pop().unwrap().0,
                    key.clone(),
                    tx.clone(),
                    find_node,
                );
                concurrent_thread_count += 1;
            }
        }

        // loop until we could not find a closer node for a round or if no threads are running
        while concurrent_thread_count > 0 {
            while concurrent_thread_count < CONCURRENCY_PARAM && !queue.is_empty() {
                self.clone().spawn_find_rpc(
                    queue.pop().unwrap().0,
                    key.clone(),
                    tx.clone(),
                    find_node,
                );
                concurrent_thread_count += 1;
            }

            let mut is_terminated = true;
            let response_opt = rx.recv().unwrap();
            concurrent_thread_count -= 1;

            match response_opt {
                Some(Response {
                    payload: ResponsePayload::Nodes(nodes),
                    receiver,
                    ..
                }) => {
                    queried_nodes.insert(receiver);
                    for node_data in nodes {
                        let curr_distance = node_data.id.xor(key);

                        if !found_nodes.contains(&node_data) {
                            if curr_distance < closest_distance {
                                closest_distance = curr_distance;
                                is_terminated = false;
                            }

                            found_nodes.insert(node_data.clone());
                            let dist = node_data.id.xor(key);
                            let next = NodeDataDistancePair(node_data.clone(), dist);
                            queue.push(next.clone());
                        }
                    }
                },
                Some(Response {
                    payload: ResponsePayload::Value(value),
                    ..
                }) => return ResponsePayload::Value(value),
                _ => is_terminated = false,
            }

            if is_terminated {
                break;
            }
            debug!("CURRENT CLOSEST DISTANCE IS {:?}", closest_distance);
        }

        debug!(
            "{} TERMINATED LOOKUP BECAUSE NOT CLOSER OR NO THREADS WITH DISTANCE {:?}",
            self.node_data.addr, closest_distance,
        );

        // loop until no threads are running or if we found REPLICATION_PARAM active nodes
        while queried_nodes.len() < REPLICATION_PARAM {
            while concurrent_thread_count < CONCURRENCY_PARAM && !queue.is_empty() {
                self.clone().spawn_find_rpc(
                    queue.pop().unwrap().0,
                    key.clone(),
                    tx.clone(),
                    find_node,
                );
                concurrent_thread_count += 1;
            }
            if concurrent_thread_count == 0 {
                break;
            }

            let response_opt = rx.recv().unwrap();
            concurrent_thread_count -= 1;

            match response_opt {
                Some(Response {
                    payload: ResponsePayload::Nodes(nodes),
                    receiver,
                    ..
                }) => {
                    queried_nodes.insert(receiver);
                    for node_data in nodes {
                        if !found_nodes.contains(&node_data) {
                            found_nodes.insert(node_data.clone());
                            let dist = node_data.id.xor(key);
                            let next = NodeDataDistancePair(node_data.clone(), dist);
                            queue.push(next.clone());
                        }
                    }
                },
                Some(Response {
                    payload: ResponsePayload::Value(value),
                    ..
                }) => return ResponsePayload::Value(value),
                _ => {},
            }
        }

        let mut ret: Vec<NodeData> = queried_nodes.into_iter().collect();
        ret.sort_by_key(|node_data| node_data.id.xor(key));
        ret.truncate(REPLICATION_PARAM);
        debug!("{} -  CLOSEST NODES ARE {:#?}", self.node_data.addr, ret);
        ResponsePayload::Nodes(ret)
    }

    /// Inserts a key-value pair into the DHT.
    pub fn insert(&mut self, key: Key, value: &str) {
        if let ResponsePayload::Nodes(nodes) = self.lookup_nodes(&key, true) {
            for dest in nodes {
                let mut node = self.clone();
                let key_clone = key;
                let value_clone = value.to_string();
                thread::spawn(move || {
                    node.rpc_store(&dest, key_clone, value_clone);
                });
            }
        }
    }

    /// Gets the value associated with a particular key in the DHT. Returns `None` if the key was
    /// not found.
    pub fn get(&mut self, key: &Key) -> Option<String> {
        if let ResponsePayload::Value(value) = self.lookup_nodes(key, false) {
            Some(value)
        } else {
            None
        }
    }

    /// Returns the `NodeData` associated with the node.
    pub fn node_data(&self) -> NodeData {
        (*self.node_data).clone()
    }

    /// Kills the current node and all active threads.
    pub fn kill(&self) {
        self.protocol.send_message(&Message::Kill, &self.node_data);
    }
}
