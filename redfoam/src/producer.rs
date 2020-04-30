use std::fs::{File, OpenOptions};
use std::sync::mpsc;
use std::net::{TcpListener, TcpStream, Shutdown};
use std::io::{Read, Write};
use std::convert::TryInto;
use std::time::Duration;
use std::thread;
use std::str;

const BUFF_SIZE : usize = 1024; //todo remove and make configureable

pub struct ProducerServer {
    rx :  mpsc::Receiver<TcpStream>,
}

enum BufferState {
    Pending,
    Active,
    Closed,
}



struct ClientBuff {
    state : BufferState,
    topic : Option<String>,
    buffer : [u8 ; BUFF_SIZE],
    buff_pos : u32,
    rec_size : u32,
    rec_pos : u32,
    rec_todo : u32,
    tcp : TcpStream,
    header : String,
}

impl ClientBuff {
    fn new (stream : TcpStream) -> ClientBuff {
        ClientBuff {
            state : BufferState::Pending, 
            topic : None, 
            buffer : [0; 1024],
            buff_pos : 0,
            rec_size : 0,
            rec_pos : 0, 
            rec_todo : 0, 
            tcp : stream,
            header : String::from(""),
        }

    }

    fn process_data(&mut self) {
        println!("process data...");
    }

    fn validate_token(&mut self) {
        let st = str::from_utf8(&self.buffer[0..self.rec_size as usize]).unwrap();
        println!("header {}", st);
        self.state = BufferState::Active;
    }

    fn process_auth(&mut self) {

        println!("process auth...");
        match self.tcp.read(&mut self.buffer) {
            Ok(size) => {
                self.buff_pos += size as u32;

                if self.rec_size == 0 && self.buff_pos >= 4 {
                    self.rec_size = u32::from_le_bytes(self.buffer[0..4].try_into().expect("slice with incorrect length"));
                }

                if self.rec_size <= self.buff_pos {
                    self.validate_token();
                    self.rec_pos = self.rec_size;

                    match self.state {

                        BufferState::Active => {
                            self.tcp.write("OK :)".as_bytes()).unwrap();
                        }, 

                        _ => {
                            self.tcp.write("BAD REQUEST".as_bytes()).unwrap();
                            self.state = BufferState::Closed;
                            self.tcp.shutdown(Shutdown::Both).expect("shutdown call failed");
                        }

                    }
                } else {
                    println!("got {} bytes - need {}", self.buff_pos, self.rec_size);
                }
                 
            },

            Err(e) => {
                println!("  OOOPSS"); 
            }
        }
    }
}


impl ProducerServer {

    pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ProducerServer {
        ProducerServer { rx }
    }

    pub fn run (&self) { 
        let mut client_list : Vec<ClientBuff> = Vec::new();

        loop {
            // add new client if sent
            let message = self.rx.try_recv();

            match message {
                Err(mpsc::TryRecvError::Empty) => {
                    //no new stream do nothing
                },

                Ok(instream) => {
                    println!("creating new client");
                    let mut c = ClientBuff::new(instream);
                    client_list.push(c);
                },
                
                Err(e) => {
                    println!("  OOOPSS");  //todo: proper error handling
                }
            }

            // process existing clients - go backwards as list will change length
            for i in (0..client_list.len()).rev() {
                let mut c = &mut client_list[i];

                match c.state {
                    BufferState::Pending => {
                        c.process_auth();
                    },

                    BufferState::Active => {
                        c.process_data();
                    },

                    BufferState::Closed => {
                        client_list.remove(i);
                    },

                }
            }
            thread::sleep(Duration::from_millis(100))
        
        }
    }
}

//            let mut file = OpenOptions::new().append(true).open("/tmp/foo.txt").unwrap();
                //file.write(&buf[0..size]).unwrap();
