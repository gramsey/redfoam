use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use super::producer::{ProducerServer};

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
