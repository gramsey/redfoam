use std::net::{TcpStream};
use std::io::Write;
use super::topic::{TopicList};
use super::buff::{Buff};
use super::tcp::{BufferState, RecordType};
use super::auth::Auth;
use super::er::Er;

pub struct ProducerClient {
    state : BufferState,
    buff : Buff,
    tcp : TcpStream,
    auth : Option<Auth>,
    rec_type : Option<RecordType>,
    topic_id : Option<u16>,
}
impl ProducerClient {
    pub fn new (stream : TcpStream) -> ProducerClient {
        let buff = Buff::new();

        ProducerClient {
            state : BufferState::Pending, 
            buff : buff,
            tcp : stream,
            auth : None,
            rec_type : None, 
            topic_id : None,
        }
    }

    pub fn process(&mut self, topic_list : &mut TopicList) -> Result<(),Er> {

        self.buff.read_data(&mut self.tcp)?; 
        if self.buff.rec_size.is_none() { self.buff.rec_size = self.buff.read_u32(); }
        self.buff.check_seq()?;
        if self.rec_type.is_none() { self.rec_type = self.buff.read_u8().map(|r| r.into()) }

        match self.rec_type {
            Some(RecordType::Auth) => {
                self.auth = Auth::new(&mut self.buff)?;
                if self.auth.is_some() {
                        self.state = BufferState::Active;
                        self.rec_type = None;
                        self.buff.reset();
                }
                Ok(())
            }, 

            Some(RecordType::Producer) => {
                if self.auth.is_some() {
                    if self.topic_id.is_none() { self.topic_id = self.buff.read_u16(); }

                    if self.buff.has_data() {
                        if let Some(topic_id) = self.topic_id {
                            topic_list.write(topic_id, self.buff.data());

                            if self.buff.is_end_of_record() {
                                let idx = topic_list.end_record(topic_id);
                                self.tcp.write(&[self.buff.seq]).unwrap();
                                self.tcp.write(&idx.to_le_bytes()).unwrap();
                                self.rec_type = None;
                                self.topic_id = None;
                            }
                            self.buff.reset();
                        }
                    }
                }
                Ok(())
            },
            _ => { Ok(()) },
        }
    }

    pub fn state(&self) -> &BufferState {
        &self.state
    }
}

