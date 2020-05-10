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
    seq : u8,
    rec_type : u8,
    topic_id : u16,
}
impl ProducerClient {
    fn new (stream : TcpStream) -> ProducerClient {
        let buff = Buff::new();

        ProducerClient {
            state : BufferState::Pending, 
            buff : buff,
            tcp : stream,
            seq : 0,
            rec_type : 0, 
            topic_id : 0,
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

    fn read_header(&mut self) {
        if self.buff.rec_size == 0 && self.buff.has_bytes_to_read(8) {
            println!("reading header");

            self.buff.rec_size = self.buff.read_u32();

            let expected_seq = self.seq % 255 + 1;
            self.seq = self.buff.read_u8();
            if expected_seq != self.seq { panic!("message has invalid sequence") }

            self.rec_type = self.buff.read_u8();
            self.topic_id = self.buff.read_u16();

            println!("size {} , seq {} , rec_type {} , topic_id {}", self.buff.rec_size, self.seq, self.rec_type, self.topic_id);
        }
    }

    fn process_data(&mut self, topic_list : &mut TopicList) {
        println!("process data...");
        self.buff.read_data(&mut self.tcp);
        self.read_header();

        if self.buff.has_data() {
            topic_list.write(self.topic_id, self.buff.data());

            if self.buff.is_end_of_record() {
                let idx = topic_list.end_record(self.topic_id);
                self.tcp.write(&[self.seq]).unwrap();
                self.tcp.write(&idx.to_le_bytes()).unwrap();
            }
            self.buff.reset();
        }
    }

    fn process_auth(&mut self) {

        self.buff.read_data(&mut self.tcp);
        println!("process_auth, rec_size {}, 6 bytes to read {} ", self.buff.rec_size, self.buff.has_bytes_to_read(6));
        
        if self.buff.rec_size == 0 && self.buff.has_bytes_to_read(6) {
            println!("reading header");
            self.buff.rec_size = self.buff.read_u32();
            self.rec_type = self.buff.read_u8();
            self.seq = self.buff.read_u8();
        }

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
    client_list : Vec<ProducerClient>,
    topic_list : TopicList,
}
impl ProducerServer {
    pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ProducerServer {
        ProducerServer { 
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
                    let c = ProducerClient::new(instream);
                    self.client_list.push(c);
                },
                Err(mpsc::TryRecvError::Empty) => { }, // no new stream - do nothing
                Err(_e) => { panic!("  OOOPSS");  }
            }

            self.client_list.retain(|c| match c.state() {
                BufferState::Closed => false, _ => true 
            });

            for c in self.client_list.iter_mut() {
                c.process(&mut self.topic_list);
            }


            thread::sleep(Duration::from_millis(100))
        }
    }
}

