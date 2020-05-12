use std::net::{TcpStream};
use std::io::Write;

use super::topic::{TopicList};
use super::buff::{Buff};
use super::tcp;
use super::tcp::BufferState;
use super::auth::Auth;
use super::er::Er;


pub struct ProducerClient {
    state : BufferState,
    buff : Buff,
    tcp : TcpStream,
    seq : u8,
    rec_type : u8,
    topic_id : u16,
}
impl ProducerClient {
    pub fn new (stream : TcpStream) -> ProducerClient {
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

    pub fn process(&mut self, topic_list : &mut TopicList) -> Result<(),Er> {

        self.buff.read_data(&mut self.tcp)?; 
        
        println!("check for header");
        if let Some((rec_type, seq)) = tcp::read_header(&mut self.buff, &self.seq) {
            self.rec_type = rec_type;
            self.seq = seq;
        }
        println!("process, rec_size {}, rec_type {}, seq {}, ", self.buff.rec_size, self.rec_type, self.seq);

        match self.state() {
            BufferState::Pending => {
                let _auth = Auth::new(&mut self.buff)?;
                println!("auth ok");
                self.state = BufferState::Active;
                self.buff.reset();
                Ok(())
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
                Ok(())
            },
            BufferState::Closed => Err(Er::IsClosed)
        }
    }

    pub fn state(&self) -> &BufferState {
        &self.state
    }
}

