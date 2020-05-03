use std::fs::{File, OpenOptions};
use std::sync::mpsc;
use std::net::{TcpStream, Shutdown};
use std::io::{Read, Write};
use std::convert::TryInto;
use std::time::Duration;
use std::thread;
use std::str;
use std::collections::HashMap;

const BUFF_SIZE : usize = 1024; //todo remove and make configureable

enum BufferState {
    Pending,
    Active,
    Closed,
}

struct Topic {
    index : u32,
    data_file : File,
    index_file : File,
    current_producer : Option<u32>,
}
impl Topic {
    // new handle, topic must already exist as a sym link
    fn new (name : String) -> Topic  {
        let data_fname = format!("/tmp/{}/data", name);
        let index_fname = format!("/tmp/{}/index", name);

        let f_data = OpenOptions::new().append(true).open(data_fname).expect("cant open topic data file");
        let f_index = OpenOptions::new().append(true).open(index_fname).expect("cant open topic index file");

        Topic {
            index : 1, //todo: read from last written + 1
            data_file : f_data,
            index_file : f_index,
            current_producer : None,
        }
    }

    fn write (&mut self, client : &ClientBuff) -> usize {
        let start = client.write_start();
        let end = client.buff_pos as usize;
        println!("writing {} to {}", start, end);
        println!("content :_{}_", str::from_utf8(&client.buffer[start..end]).expect("failed to format"));
        self.data_file.write( &client.buffer[start..end]).unwrap()
    }
}

struct ClientBuff {
    producer_id : u32,
    state : BufferState,
    topic : Option<String>,
    buffer : [u8 ; BUFF_SIZE],
    buff_pos : u32,
    rec_size : u32,
    rec_pos : u32,
    tcp : TcpStream,
}
impl ClientBuff {
    fn new (stream : TcpStream, producer_id : u32) -> ClientBuff {
        ClientBuff {
            producer_id : producer_id,
            state : BufferState::Pending, 
            topic : None, 
            buffer : [0; 1024],
            buff_pos : 0,
            rec_size : 0,
            rec_pos : 0, 
            tcp : stream,
        }

    }

    fn write_start(&self) -> usize {
        self.rec_pos as usize % BUFF_SIZE 
    }

    fn process_data(&mut self, topic_list : &mut HashMap<String,Topic>) {
        println!("process data...");

        if self.buff_pos as usize >= self.write_start() {  
            match &self.topic {
                Some(topic_name) => match topic_list.get_mut(topic_name) {
                    Some(topic) => {
                        let n = topic.write(&self);
                        self.rec_pos += n as u32;
                        
                        // if record is completely written reset
                        if self.rec_pos == self.rec_size {
                            self.rec_pos = 0;
                            self.rec_size = 0;
                            topic.current_producer = None;
                            topic.index += 1;
                        } else if self.rec_pos < self.rec_size {
                            topic.current_producer = Some(self.producer_id); // record only half written need to block topic
                        } else {
                            panic!("read past end of record? shouldn't happen...");
                        }

                        println!("adding {} to rec_pos to get {} ", n, self.rec_pos);
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


        match self.tcp.read(&mut self.buffer[self.buff_pos as usize..1024]) {
            Ok(size) => {
                self.buff_pos += size as u32;

                if self.rec_size == self.rec_pos && self.buff_pos >= 4 {
                    self.rec_size = u32::from_le_bytes(self.buffer[0..4].try_into().expect("slice with incorrect length"));
                }
            },

            Err(_e) => {
                println!("  OOOPSS"); 
            }
        }
    }

    fn validate_token(&mut self) {
        println!("validating token");

      
        let st = str::from_utf8(&self.buffer[4..self.rec_size as usize]).unwrap();

        for x in st.chars() {
            println!("{}",x);
        }
        println!("done split");
        println!("splitting :  {}", st);
        let mut split = st.split(";");


        let topic_name = String::from(split.next().unwrap());
        println!("topic : {}", &topic_name);
        self.topic = Some(topic_name);


        let auth_token = split.next().unwrap();
        println!("auth_token : {}", auth_token);
        

        if auth_token == "ANON" {
            println!("header  {}, length {}, buff_pos {}", st, self.rec_size, self. buff_pos);
            self.state = BufferState::Active;
        } else {
            println!("AUTH failed expected ANON got {}", st);
            self.tcp.write("BAD REQUEST".as_bytes()).unwrap();
            self.state = BufferState::Closed;
            self.tcp.shutdown(Shutdown::Both).expect("shutdown call failed");
        }
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
                } else {
                    println!("got {} bytes - need {}", self.buff_pos, self.rec_size);
                }
                 
            },

            Err(_e) => {
                println!("  OOOPSS"); 
            }
        }
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

