use std::net::{TcpStream};
//use std::io::Write;
use std::ffi::OsStr;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::topic::{TopicList};
use super::buff::{Buff};
use super::tcp::{BufferState, RecordType};
use super::auth::Auth;
use super::er::Er;

pub struct ConsumerClient {
    id : u32,
    state : BufferState,
    buff : Buff,
    tcp : TcpStream,
    auth : Option<Auth>,
    rec_type : Option<RecordType>,
}
impl ConsumerClient {
    pub fn new (id : u32, stream : TcpStream) -> ConsumerClient {
        let buff = Buff::new();

        ConsumerClient {
            id : id,
            state : BufferState::Pending, 
            buff : buff,
            tcp : stream,
            auth : None,
            rec_type : None, 
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

            Some(RecordType::ConsumerStart) => {
                if self.auth.is_some() {

                    if self.buff.has_data() {
                        /*
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
                        */
                    }
                }
                Ok(())
            },

            Some(RecordType::ConsumerFollowTopics) => {
                if self.auth.is_some() {
                    if self.buff.has_data() {
                        if self.buff.is_end_of_record() {
                            topic_list.follow_topics(self.buff.data())?;
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

    pub fn send_index(&self, _buffer : &[u8]) {

    }

    pub fn send_data(&self, _buffer : &[u8]) {

    }

    pub fn state(&self) -> &BufferState {
        &self.state
    }
}


pub struct ConsumerServer {
    rx :  mpsc::Receiver<TcpStream>,
    client_list : HashMap<u32, ConsumerClient>,
    topic_list : TopicList,
    next_client_id : u32,
}
impl ConsumerServer {
    pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ConsumerServer {

        let client_list : HashMap<u32, ConsumerClient> = HashMap::new();
        let topic_list = TopicList::new(false);

        ConsumerServer {
            rx,
            client_list,
            topic_list,
            next_client_id : 0,
        }
    }

    pub fn run (&mut self) { 
        //let mut client_list : Vec<ClientBuffx> = Vec::new();

        loop {
            // add new client if sent
            let message = self.rx.try_recv();

            self.next_client_id += 1;

            match message {
                Err(mpsc::TryRecvError::Empty) => {
                    //no new stream do nothing
                },

                Ok(instream) => {
                    println!("creating new client");
                    let c = ConsumerClient::new(self.next_client_id, instream);
                    self.client_list.insert(self.next_client_id, c);
                },
                
                Err(_e) => {
                    println!("  OOOPSS");  //todo: proper error handling
                }
            }

            // process existing clients - go backwards as list will change length
            for (_, client) in &mut self.client_list {
                client.process(&mut self.topic_list);
            }

            self.client_list.retain(| _, c | match c.state() {
                BufferState::Closed => false, _ => true 
            });


            // process topic updates
            let mut event_buffer = [0; 1024];
            let mut buffer = [0; 1024];
            let events = self.topic_list.notify.read_events(&mut event_buffer)
                .expect("Error while reading events");
            let index_str = OsStr::new("index");
            let data_str = OsStr::new("data");

            for e in events {
                match self.topic_list.watchers.get(&e.wd) {
                    Some(topic_id) => {
                        match e.name {
                            Some(i) if i == index_str => {
                                let tid = *topic_id;
                                self.topic_list.read_index(tid, &mut buffer);
                                if let Some(client_ids) = self.topic_list.followers.get_mut(&tid) {
                                    for client_id in client_ids {
                                        if let Some(client) = self.client_list.get_mut(client_id) {
                                            client.send_index(&buffer);
                                        }
                                    }
                                }
                            },
                            Some(d) if d == data_str => {

                            }, 
                            Some(_) => {/* error neither data nor index file */}
                            None => { /* error "event should have a file name" */},
                        }
                    },
                    None => assert!(false, "topic id missing from watcher list"),
                }
            }


            thread::sleep(Duration::from_millis(100))
        }
    }
}

