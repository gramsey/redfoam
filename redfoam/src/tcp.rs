use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;

use super::producer::{ProducerServer};
use super::buff::Buff;


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
