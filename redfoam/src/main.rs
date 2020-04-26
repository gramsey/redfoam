#![warn(rust_2018_idioms)]

// use tokio;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::net::TcpListener;
// use tokio::fs::File;

use std::fs::{File, OpenOptions};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::io::{Read, Write};

use std::env;
use std::error::Error;


fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 1024];
    let mut file = OpenOptions::new().append(true).open("/tmp/foo.txt").unwrap();

    while match stream.read(&mut buf) {
        Ok(size) => {

            stream.write(&buf[0..size]).unwrap();

            file.write(&buf[0..size]).unwrap();

            true
        },

        Err(_) => {
            println!("Error with tcp stream to {}, (terminating connection)", stream.peer_addr().unwrap());
            stream.shutdown(Shutdown::Both).unwrap();

            false
        }
    } {}
}


fn main() {
    let addr = env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:9090".to_string());

    let listener = TcpListener::bind(&addr).unwrap();
    println!("Listening on: {}", addr);


    for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("new client! {}", stream.peer_addr().unwrap());
                    handle_client(stream);
                }

                Err(e) => {
                    panic!("connection failed");
                }
            }
    }

}
