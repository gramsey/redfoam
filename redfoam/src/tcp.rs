use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::producer::{ProducerClient};
use super::topic::{TopicList};
use super::er::Er;

pub enum BufferState {
    Pending,
    Active,
    Closed,
}

pub enum RecordType {
    Auth,
    Producer,
    ConsumerFollowTopics,
    Undefined,
    DataFeed,
    IndexFeed,
}

impl From<u8> for RecordType {
    fn from(code : u8) -> Self {
        match code {
            1 => Self::Auth,
            2 => Self::Producer,
            3 => Self::ConsumerFollowTopics,
            4 => Self::DataFeed,
            5 => Self::IndexFeed,
            _ => Self::Undefined,
        }
    }

}

macro_rules! make_server {
    ($typename: ident, $handler : ty) => {
        pub struct $typename {
            rx :  mpsc::Receiver<TcpStream>,
            client_list : Vec<$handler>,
            topic_list : TopicList,
        }
        impl $typename {
            pub fn new (rx :  mpsc::Receiver<TcpStream>) -> $typename {
                $typename { 
                    rx : rx,
                    client_list : Vec::new(),
                    topic_list : TopicList::new(),
                }
            }

            pub fn run (&mut self) { 
                loop {

                    match self.rx.try_recv() {
                        Ok(instream) => {
                            println!("creating new client");
                            let c = <$handler>::new(instream);
                            self.client_list.push(c);
                        },
                        Err(mpsc::TryRecvError::Empty) => { }, // no new stream - do nothing
                        Err(_e) => { panic!("  OOOPSS");  }
                    }

                    self.client_list.retain(|c| match c.state() {
                        BufferState::Closed => false, _ => true 
                    });

                    for c in self.client_list.iter_mut() {
                        if let Err(e) = c.process(&mut self.topic_list) {
                            match e { 
                                Er::NotReady   => {println!("Error : {}", e);},
                                _               => {println!("Error : {}", e);},
                            }
                        }
                    }

                    thread::sleep(Duration::from_millis(100))
                }
            }
        }
    }
}

make_server!(ProducerServer, ProducerClient);

pub fn run_server(addr : String) {

    let (tx, rx) : (mpsc::Sender<TcpStream>, mpsc::Receiver<TcpStream>) = mpsc::channel();

    thread::spawn(move || {
        ProducerServer::new(rx).run();
    });


    let listener = TcpListener::bind(&addr).unwrap();
    println!("Listening on: {}", addr);


    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                stream.set_nonblocking(true).expect("set_nonblocking call failed");
                println!("sending stream");
                tx.send(stream).unwrap();
                println!("sent stream");
            },

            Err(_e) => {
                panic!("connection failed");
            }
        }
    }
}
