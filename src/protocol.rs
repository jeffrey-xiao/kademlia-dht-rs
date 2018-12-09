use crate::key::Key;
use crate::node::node_data::NodeData;
use crate::MESSAGE_LENGTH;
use bincode;
use log::{log, warn};
use serde_derive::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::str;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;

/// An enum representing a request RPC.
///
/// Each request RPC also carries a randomly generated key. The response to the RPC must contain
/// the same randomly generated key or else it will be ignored.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub id: Key,
    pub sender: NodeData,
    pub payload: RequestPayload,
}

/// An enum representing the payload to a request RPC.
///
/// As stated in the Kademlia paper, the four possible RPCs are `PING`, `STORE`, `FIND_NODE`, and
/// `FIND_VALUE`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestPayload {
    Ping,
    Store(Key, String),
    FindNode(Key),
    FindValue(Key),
}

/// An enum representing the response to a request RPC.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub request: Request,
    pub receiver: NodeData,
    pub payload: ResponsePayload,
}

/// An enum representing the payload to a response RPC.
///
/// As stated in the Kademlia paper, a response to a request could be a list of nodes, a value, or
/// a pong.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponsePayload {
    Nodes(Vec<NodeData>),
    Value(String),
    Pong,
}

/// An enum that represents a message that is sent between nodes.
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
    Kill,
}

/// `Protocol` facilitates the underlying communication between nodes by sending messages to other
/// nodes, and by passing messages from other nodes to the current node.
#[derive(Clone)]
pub struct Protocol {
    socket: Arc<UdpSocket>,
}

impl Protocol {
    pub fn new(socket: UdpSocket, tx: Sender<Message>) -> Protocol {
        let protocol = Protocol {
            socket: Arc::new(socket),
        };
        let ret = protocol.clone();
        thread::spawn(move || {
            let mut buffer = [0u8; MESSAGE_LENGTH];
            loop {
                let (len, _src_addr) = protocol.socket.recv_from(&mut buffer).unwrap();
                let message = bincode::deserialize(&buffer[..len]).unwrap();

                if tx.send(message).is_err() {
                    warn!("Protocol: Connection closed.");
                    break;
                }
            }
        });
        ret
    }

    pub fn send_message(&self, message: &Message, node_data: &NodeData) {
        let size_limit = bincode::Bounded(MESSAGE_LENGTH as u64);
        let buffer_string = bincode::serialize(&message, size_limit).unwrap();
        let NodeData { ref addr, .. } = node_data;
        if self.socket.send_to(&buffer_string, addr).is_err() {
            warn!("Protocol: Could not send data.");
        }
    }
}
