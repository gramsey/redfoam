use std::sync::mpsc;
use std::net::{TcpStream};
use std::time::Duration;
use std::thread;
use std::io::Write;

use super::topic::{TopicList};
use super::buff::{Buff};
use super::tcp;
use super::auth::Auth;
use super::er::Er;

enum BufferState {
    Pending,
    Active,
    Closed,
}

make_server!(ProducerServer, ProducerClient);

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
        match self.buff.read_data(&mut self.tcp) {
            Ok(_) | Err(Er::NotReady) => {},
            Err(_) => {panic!("error reading tcp");}
        };
        
        println!("check for header");
        if let Some((rec_type, seq)) = tcp::read_header(&mut self.buff, &self.seq) {
            self.rec_type = rec_type;
            self.seq = seq;
        }
        println!("process, rec_size {}, rec_type {}, seq {}, ", self.buff.rec_size, self.rec_type, self.seq);

        match self.state() {
            BufferState::Pending => {
                match Auth::new(&mut self.buff) {
                    Ok(_) => {
                        self.state = BufferState::Active;
                    }, 

                    Err(Er::NotReady) =>  {},

                    Err(_) => {
                        self.state = BufferState::Closed;
                        panic!("problem with auth");
                    }, 
                }
                self.buff.reset();
            }, 

            BufferState::Active => {
                println!("process data...");

                if self.rec_type == 2 && self.topic_id == 0 && self.buff.has_bytes_to_read(2) {
                    println!("setting topic_id");
                    self.topic_id = self.buff.read_u16();
                }

                if self.buff.has_data() && self.topic_id != 0 {
                    println!("writing data to file");
                    topic_list.write(self.topic_id, self.buff.data());

                    if self.buff.is_end_of_record() {
                        let idx = topic_list.end_record(self.topic_id);
                        self.tcp.write(&[self.seq]).unwrap();
                        self.tcp.write(&idx.to_le_bytes()).unwrap();
                        self.rec_type = 0;
                        self.topic_id = 0;
                    }
                    self.buff.reset();
                }
            },
            BufferState::Closed => (),
        }
    }

    fn state(&self) -> &BufferState {
        &self.state
    }
}

