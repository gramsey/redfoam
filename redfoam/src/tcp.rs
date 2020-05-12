use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::producer::{ProducerClient};
use super::topic::{TopicList};
use super::buff::Buff;
use super::er::Er;

pub enum BufferState {
    Pending,
    Active,
    Closed,
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

pub fn read_header(buff : &mut Buff, old_seq : &u8) -> Option<(u8, u8)> {
    if buff.rec_size == 0 && buff.has_bytes_to_read(6) {
        println!("reading header");

        buff.rec_size = buff.read_u32();

        let expected_seq = old_seq % 255 + 1;
        let seq = buff.read_u8();
        if expected_seq != seq { panic!("message has invalid sequence") }

        let rec_type = buff.read_u8();
        println!("rec-type {}, size {}, seq {}", rec_type, buff.rec_size, seq);
        Some((rec_type, seq))
    } else {
        println!("not enough data");
        None
    }
}
