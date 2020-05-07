use std::sync::mpsc;
use std::net::{TcpStream, Shutdown};
use std::time::Duration;
use std::thread;
use std::io::Write;
use std::str;
use std::collections::HashMap;
use super::topic::{Topic};
use super::buff::{Buff};

enum BufferState {
    Pending,
    Active,
    Closed,
}

struct ProducerClient {
    state : BufferState,
    topic : Option<String>,
    buff : Buff,
    tcp : TcpStream,
}
impl ProducerClient {
    fn new (stream : TcpStream) -> ProducerClient {
        let buff = Buff::new();

        ProducerClient {
            state : BufferState::Pending, 
            buff : buff,
            topic : None, 
            tcp : stream,
        }
    }

    fn process_data(&mut self, topic_list : &mut HashMap<String,Topic>) {
        println!("process data...");
        self.buff.read_data(&mut self.tcp);
        if self.buff.has_data() {
            match &self.topic {
                Some(topic_name) => match topic_list.get_mut(topic_name) {
                    Some(topic) => {
                        let written = topic.write(self.buff.data());
                        if self.buff.is_end_of_record() {
                            topic.end_rec();
                        }
                        self.buff.reset();
                        println!(" wrote {}", written);
                    },

                    None => {
                        panic!("invalid topic");
                    },
                },

                None => {
                    panic!("topic not set for authorized client");
                }
            }
        }
    }

    fn process_auth(&mut self) {

        self.buff.read_data(&mut self.tcp);

        if self.buff.is_end_of_record() {
            let content = str::from_utf8(self.buff.data()).unwrap();
            println!("content : {} ", content);

            let mut authpart = content.split(";");

            let topic_name = String::from(authpart.next().unwrap());
            println!("topic : {}", &topic_name);
            self.topic = Some(topic_name);

            let auth_token = authpart.next().unwrap();
            println!("auth_token : {}", auth_token);

            if auth_token == "ANON" {
                self.state = BufferState::Active;
                self.buff.reset();
            } else {
                println!("AUTH failed expected ANON got {}", auth_token);
                self.tcp.write("BAD REQUEST".as_bytes()).unwrap();
                self.state = BufferState::Closed;
                self.tcp.shutdown(Shutdown::Both).expect("shutdown call failed");
            }
        }
    }

    fn state(&self) -> &BufferState {
        &self.state
    }
}

pub struct ProducerServer {
    rx :  mpsc::Receiver<TcpStream>,
}
impl ProducerServer {
    pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ProducerServer {
        ProducerServer { rx }
    }

    pub fn run (&self) { 
        let mut client_list : Vec<ProducerClient> = Vec::new();
        let mut topic_list : HashMap<String, Topic> = HashMap::new();
        topic_list.insert(String::from("test"), Topic::new(String::from("test")));

        loop {
            // add new client if sent
            let message = self.rx.try_recv();

            match message {
                Err(mpsc::TryRecvError::Empty) => {
                    //no new stream do nothing
                },

                Ok(instream) => {
                    println!("creating new client");
                    let c = ProducerClient::new(instream);
                    client_list.push(c);
                },
                
                Err(_e) => {
                    println!("  OOOPSS");  //todo: proper error handling
                }
            }

            // process existing clients - go backwards as list will change length
            for i in (0..client_list.len()).rev() {
                let c = &mut client_list[i];

                match c.state() {
                    BufferState::Pending => {
                        c.process_auth();
                    },

                    BufferState::Active => {
                        c.process_data(&mut topic_list);
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

