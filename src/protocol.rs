use bincode;
use std::net::UdpSocket;
use std::str;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread;

use MESSAGE_LENGTH;
use node::node_data::NodeData;
use key::Key;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub id: Key,
    pub sender: NodeData,
    pub payload: RequestPayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestPayload {
    Ping,
    Store(Key, String),
    FindNode(Key),
    FindValue(Key),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub request: Request,
    pub receiver: NodeData,
    pub payload: ResponsePayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponsePayload {
    Nodes(Vec<NodeData>),
    Value(String),
    Pong,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
    Kill,
}

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
        let buffer_string = bincode::serialize(&message, bincode::Bounded(MESSAGE_LENGTH as u64)).unwrap();
        let &NodeData { ref addr, .. } = node_data;
        if self.socket.send_to(&buffer_string, addr).is_err() {
            warn!("Protocol: Could not send data.");
        }
    }
}
