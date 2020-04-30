#![warn(rust_2018_idioms)]

// use tokio;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::net::TcpListener;
// use tokio::fs::File;

use std::net::{TcpListener, TcpStream, Shutdown};
use std::io::{Read, Write};

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use std::env;
use std::error::Error;
use redfoam::producer::{ProducerServer};

fn main() {
    let addr = env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:9090".to_string());

    let (tx, rx) : (mpsc::Sender<TcpStream>, mpsc::Receiver<TcpStream>) = mpsc::channel();

    thread::spawn(move || {
        ProducerServer::new(rx).run();
    });


    let listener = TcpListener::bind(&addr).unwrap();
    println!("Listening on: {}", addr);


    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                tx.send(stream).unwrap();
            },

            Err(e) => {
                panic!("connection failed");
            }
        }
    }

}
