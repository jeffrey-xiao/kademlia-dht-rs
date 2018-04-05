pub mod node_data;

use std::cmp;
use std::net::UdpSocket;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use {BUCKET_REFRESH_INTERVAL, CONCURRENCY_PARAM, KEY_LENGTH, REPLICATION_PARAM, REQUEST_TIMEOUT};
use node::node_data::{NodeData, NodeDataDistancePair};
use routing::RoutingTable;
use protocol::{Message, Protocol, Request, RequestPayload, Response, ResponsePayload};
use key::Key;
use storage::Storage;

#[derive(Clone)]
pub struct Node {
    pub node_data: Arc<NodeData>,
    routing_table: Arc<Mutex<RoutingTable>>,
    storage: Arc<Mutex<Storage>>,
    pending_requests: Arc<Mutex<HashMap<Key, Sender<Response>>>>,
    pub protocol: Arc<Protocol>,
    is_active: Arc<AtomicBool>,
}

impl Node {
    pub fn new(ip: &str, port: &str, bootstrap: Option<NodeData>) -> Self {
        let socket = UdpSocket::bind(format!("{}:{}", ip, port))
            .expect("Error: Could not bind to address!");
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
            node_data: node_data,
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

    fn bootstrap_routing_table(&mut self) {
        let target_key = self.node_data.id;
        self.lookup_nodes(&target_key, true);

        let bucket_size = { self.routing_table.lock().unwrap().size() };

        for i in 0..bucket_size {
            self.lookup_nodes(&Key::rand_in_range(i), true);
        }
    }

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

    fn handle_request(&mut self, request: &Request) {
        info!(
            "{} - Receiving request from {} {:#?}",
            self.node_data.addr,
            request.sender.addr,
            request.payload,
        );
        self.clone().update_routing_table(request.sender.clone());
        let receiver = (*self.node_data).clone();
        let payload = match request.payload.clone() {
            RequestPayload::Ping => ResponsePayload::Pong,
            RequestPayload::Store(key, value) => {
                self.storage.lock().unwrap().insert(key, value);
                ResponsePayload::Pong
            },
            RequestPayload::FindNode(key) => ResponsePayload::Nodes(
                self.routing_table.lock().unwrap().get_closest_nodes(&key, REPLICATION_PARAM),
            ),
            RequestPayload::FindValue(key) => {
                if let Some(value) = self.storage.lock().unwrap().get(&key) {
                    ResponsePayload::Value(value.clone())
                } else {
                    ResponsePayload::Nodes(
                        self.routing_table.lock().unwrap().get_closest_nodes(&key, REPLICATION_PARAM),
                    )
                }
            },
        };

        self.protocol.send_message(
            &Message::Response(Response {
                request: request.clone(),
                receiver: receiver,
                payload,
            }),
            &request.sender,
        )
    }

    fn handle_response(&mut self, response: &Response) {
        self.clone().update_routing_table(response.receiver.clone());
        let pending_requests = self.pending_requests.lock().unwrap();
        let Response { ref request, .. } = response.clone();
        if let Some(sender) = pending_requests.get(&request.id) {
            info!(
                "{} - Receiving response from {} {:#?}",
                self.node_data.addr,
                response.receiver.addr,
                response.payload,
            );
            sender.send(response.clone()).unwrap();
        } else {
            warn!("{} - Original request not found; irrelevant response or expired request.", self.node_data.addr);
        }
    }

    fn send_request(&mut self, dest: &NodeData, payload: RequestPayload) -> Option<Response> {
        info!("{} - Sending request to {} {:#?}", self.node_data.addr, dest.addr, payload);
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
                payload: payload,
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
                warn!("{} - Request to {} timed out after waiting for {} milliseconds", self.node_data.addr, dest.addr, REQUEST_TIMEOUT);
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                let mut routing_table = self.routing_table.lock().unwrap();
                routing_table.remove_node(dest);
                None
            },
        }
    }

    fn rpc_ping(&mut self, dest: &NodeData) -> Option<Response> {
        self.send_request(dest, RequestPayload::Ping)
    }

    fn rpc_store(&mut self, dest: &NodeData, key: Key, value: String) -> Option<Response> {
        self.send_request(dest, RequestPayload::Store(key, value))
    }

    fn rpc_find_node(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindNode(*key))
    }

    fn rpc_find_value(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindValue(*key))
    }

    fn spawn_find_rpc(mut self, dest: NodeData, key: Key, sender: Sender<Option<Response>>, find_node: bool) {
        thread::spawn(move || {
            let find_node_err = find_node && sender.send(self.rpc_find_node(&dest, &key)).is_err();
            let find_value_err = !find_node && sender.send(self.rpc_find_value(&dest, &key)).is_err();
            if find_node_err || find_value_err {
                warn!("Receiver closed channel before rpc returned.");
            }
        });
    }

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
                self.clone().spawn_find_rpc(queue.pop().unwrap().0, key.clone(), tx.clone(), find_node);
                concurrent_thread_count += 1;
            }
        }

        // loop until we could not find a closer node for a round or if no threads are running
        while concurrent_thread_count > 0 {
            while concurrent_thread_count < CONCURRENCY_PARAM && !queue.is_empty() {
                self.clone().spawn_find_rpc(queue.pop().unwrap().0, key.clone(), tx.clone(), find_node);
                concurrent_thread_count += 1;
            }

            let mut is_terminated = true;
            let response_opt = rx.recv().unwrap();
            concurrent_thread_count -= 1;

            match response_opt {
                Some(Response { payload: ResponsePayload::Nodes(nodes), receiver, .. }) => {
                    queried_nodes.insert(receiver);
                    for node_data in nodes {
                        let curr_distance = node_data.id.xor(key);

                        if !found_nodes.contains(&node_data) {
                            if curr_distance < closest_distance {
                                closest_distance = curr_distance;
                                is_terminated = false;
                            }

                            found_nodes.insert(node_data.clone());
                            let next = NodeDataDistancePair(node_data.clone(), node_data.id.xor(key));
                            queue.push(next.clone());
                        }
                    }
                },
                Some(Response { payload: ResponsePayload::Value(value), .. }) => {
                    return ResponsePayload::Value(value);
                },
                _ => {
                    is_terminated = false;
                },
            }

            if is_terminated {
                break;
            }
            debug!("CURR CLOSEST DISTANCE IS {:?}", closest_distance);
        }

        debug!(
            "{} TERMINATED LOOKUP BECAUSE NOT CLOSER OR NO THREADS WITH DISTANCE {:?}",
            self.node_data.addr,
            closest_distance,
        );

        // loop until no threads are running or if we found REPLICATION_PARAM active nodes
        while queried_nodes.len() < REPLICATION_PARAM {
            while concurrent_thread_count < CONCURRENCY_PARAM && !queue.is_empty() {
                self.clone().spawn_find_rpc(queue.pop().unwrap().0, key.clone(), tx.clone(), find_node);
                concurrent_thread_count += 1;
            }
            if concurrent_thread_count == 0 {
                break;
            }

            let response_opt = rx.recv().unwrap();
            concurrent_thread_count -= 1;

            match response_opt {
                Some(Response { payload: ResponsePayload::Nodes(nodes), receiver, .. }) => {
                    queried_nodes.insert(receiver);
                    for node_data in nodes {
                        if !found_nodes.contains(&node_data) {
                            found_nodes.insert(node_data.clone());
                            let next = NodeDataDistancePair(node_data.clone(), node_data.id.xor(key));
                            queue.push(next.clone());
                        }
                    }
                },
                Some(Response { payload: ResponsePayload::Value(value), .. }) => {
                    return ResponsePayload::Value(value);
                },
                _ => {},
            }
        }

        let mut ret: Vec<NodeData> = queried_nodes.into_iter().collect();
        ret.sort_by_key(|node_data| node_data.id.xor(key));
        ret.truncate(REPLICATION_PARAM);
        debug!("{} -  CLOSEST NODES ARE {:#?}", self.node_data.addr, ret);
        ResponsePayload::Nodes(ret)
    }

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

    pub fn get(&mut self, key: &Key) -> Option<String> {
        if let ResponsePayload::Value(value) = self.lookup_nodes(key, false) {
            Some(value)
        } else {
            None
        }
    }
}
