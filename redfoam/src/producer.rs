use std::sync::mpsc;
use std::net::{TcpStream, Shutdown};
use std::time::Duration;
use std::thread;
use std::io::Write;
use std::str;
use super::topic::{TopicList};
use super::buff::{Buff};

enum BufferState {
    Pending,
    Active,
    Closed,
}

struct ProducerClient {
    state : BufferState,
    buff : Buff,
    tcp : TcpStream,
}
impl ProducerClient {
    fn new (stream : TcpStream) -> ProducerClient {
        let buff = Buff::new();

        ProducerClient {
            state : BufferState::Pending, 
            buff : buff,
            tcp : stream,
        }
    }

    fn process(&mut self, topic_list : &mut TopicList) {
        match self.state() {
            BufferState::Pending => {
                self.process_auth();
            },

            BufferState::Active => {
                self.process_data(topic_list);
            },

            BufferState::Closed => {
                panic!("trying to process closed connection");
            },

        }
    }

    fn process_data(&mut self, topic_list : &mut TopicList) {
        println!("process data...");
        self.buff.read_data(&mut self.tcp);

        if self.buff.has_data() {
            topic_list.write(self.buff.topic_id(), self.buff.data());

            if self.buff.is_end_of_record() {
                let idx = topic_list.end_record(self.buff.topic_id());
                self.tcp.write(&[self.buff.seq()]).unwrap();
                self.tcp.write(&idx.to_le_bytes()).unwrap();
            }
            self.buff.reset();
        }
    }

    fn process_auth(&mut self) {

        self.buff.read_data(&mut self.tcp);

        if self.buff.is_end_of_record() {
            let content = str::from_utf8(self.buff.data()).unwrap();
            println!("content : {} ", content);

            let mut authpart = content.split(";");

            let topic_name = String::from(authpart.next().unwrap());
            println!("topic_name : {}", topic_name);


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
        let mut topic_list = TopicList::new();

        loop {
            // add new client if sent
            //
            println!("waiting for message");
            let message = self.rx.try_recv();
            println!("got message");

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
                println!("loop");
                let c = &mut client_list[i];
                match c.state() {
                    BufferState::Closed =>  {
                        client_list.remove(i);
                    },
                    _ => {
                        c.process(&mut topic_list);
                    }
                }
            }
            thread::sleep(Duration::from_millis(100))
        }
    }
}

