use std::net::{TcpStream};
use std::io::Write;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::{trace};

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
    topic_id : Option<u32>,
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
                    match self.buff.read_u32() {
                        Some(topic_id) => {
                            self.topic_id = Some(topic_id);
                            let (index_pos, data_pos) = topic_list.follow_topic(topic_id, self.id)?;
                            self.rec_type = None;
                            self.buff.reset();
                            //send response
                            let size = 17u32; // u8 + u64 + u64

                            self.tcp.write(&size.to_le_bytes())
                                .map_err(|e| Er::ServerTcpWrite(e))?;

                            self.tcp.write(&[RecordType::ConsumerFollowTopics as u8])
                                .map_err(|e| Er::ServerTcpWrite(e))?;

                            self.tcp.write(&index_pos.to_le_bytes())
                                .map_err(|e| Er::ServerTcpWrite(e))?;

                            self.tcp.write(&data_pos.to_le_bytes())
                                .map_err(|e| Er::ServerTcpWrite(e))?;
                        }, 
                        None => { /* record MUST have topic id */ },
                    }
                }
                Ok(())
            },
            _ => { Ok(()) },
        }
    }

    pub fn send_feed(&mut self, offset : u64, buffer : &[u8], feed_type : RecordType) -> Result<(),Er> {
        trace!("send_feed(offset={}, feed_type={}", offset, feed_type as u8);

        let length : u32 = buffer.len() as u32;
        trace!("   length {}", length);

        self.tcp.write(&length.to_le_bytes())
            .map_err(|e| Er::ClientTcpWrite(e))?;

        self.tcp.write(&[feed_type as u8])
            .map_err(|e| Er::ClientTcpWrite(e))?;

        self.tcp.write(buffer)
            .map_err(|e| Er::ClientTcpWrite(e))?;

        Ok(())
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
                    let c = ConsumerClient::new(self.next_client_id, instream);
                    self.client_list.insert(self.next_client_id, c);
                },
                
                Err(_e) => {
                    unimplemented!();
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
            let events = self.topic_list.notify.read_events(&mut event_buffer)
                .expect("Error while reading events");

            for e in events {
                match self.topic_list.watchers.get(&e.wd) {
                    Some(topic_id) => {
                        match e.name {
                            Some(event_name) => {
                                if let Some(file_name) = event_name.to_str() {
                                    let t_id = *topic_id;
                                    self.send_to_client(t_id, file_name);  
                                } else {
                                    unimplemented!();
                                }
                            }, 
                            None => { /* error "event should have a file name" */},
                        }
                    },
                    None => assert!(false, "topic id missing from watcher list"),
                }
            }


            thread::sleep(Duration::from_millis(100))
        }
    }

    fn send_to_client(&mut self, topic_id : u32, file_name : &str) -> Result<(), Er> {
        let mut buffer = [0; 1010];

        let feed_type = match file_name {
            "index" => RecordType::IndexFeed, 
            "data"  => RecordType::DataFeed,
            _       => RecordType::Undefined,
        };

        let (offset, size) = match feed_type {
            RecordType::IndexFeed => self.topic_list.read_index(topic_id, &mut buffer)?,
            RecordType::DataFeed => self.topic_list.read_data(topic_id, &mut buffer)?,
            _ => (0, 0),
        };

        if let Some(client_ids) = self.topic_list.followers.get_mut(&topic_id) {
            for client_id in client_ids {
                if let Some(client) = self.client_list.get_mut(client_id) {
                    client.send_feed(offset, &buffer[..size], feed_type);
                }
            }
        }
        Ok(())
    }

}

