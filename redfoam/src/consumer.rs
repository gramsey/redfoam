use std::net::{TcpStream};
use std::io::Write;
use super::topic::{TopicList};
use super::buff::{Buff};
use super::tcp::{BufferState, RecordType};
use super::auth::Auth;
use super::er::Er;

pub struct ConsumerClient {
    state : BufferState,
    buff : Buff,
    tcp : TcpStream,
    auth : Option<Auth>,
    rec_type : Option<RecordType>,
    topic_ids : Option<Vec<u16>>,
}
impl ConsumerClient {
    pub fn new (stream : TcpStream) -> ConsumerClient {
        let buff = Buff::new();

        ConsumerClient {
            state : BufferState::Pending, 
            buff : buff,
            tcp : stream,
            auth : None,
            rec_type : None, 
            topic_ids : None,
        }
    }

    pub fn process(&mut self, topic_list : &mut TopicList) -> Result<(),Er> {

        if let Some(topics) = self.topic_ids {
            for n in topics {
                if let some(t) = topic::get_topic(n) {
                    if let Some(data) = t.get_data() {
                        self.tcp.write(data);
                    }
                }
            }
        }

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

            Some(RecordType::Consumer) => {
                if self.auth.is_some() {

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

            Some(RecordType::ConsumerFollowTopics) => {
                if self.auth.is_some() {
                    if self.buff.has_data() {
                        if self.buff.is_end_of_record() {
                            let self.topics = topic::getTopics(self.buff.data())?;
                        }
                        self.buff.reset();
                        self.rec_type = None;
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


pub struct ConsumerServer {
    rx :  mpsc::Receiver<TcpStream>,
}
impl ConsumerServer {
    pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ConsumerServer {
        ConsumerServer { rx }
    }

    pub fn run (&self) { 
        let mut client_list : Vec<ClientBuff> = Vec::new();
        let mut topic_list : HashMap<String, Topic> = HashMap::new();
        topic_list.insert(String::from("test"), Topic::new(String::from("test")));
        let mut next_producerid : u32 = 0;

        loop {
            // add new client if sent
            let message = self.rx.try_recv();

            next_producerid += 1;

            match message {
                Err(mpsc::TryRecvError::Empty) => {
                    //no new stream do nothing
                },

                Ok(instream) => {
                    println!("creating new client");
                    let c = ClientBuff::new(instream, next_producerid);
                    client_list.push(c);
                },
                
                Err(_e) => {
                    println!("  OOOPSS");  //todo: proper error handling
                }
            }

            // process existing clients - go backwards as list will change length
            for i in (0..client_list.len()).rev() {
                let c = &mut client_list[i];

                match c.state {
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

