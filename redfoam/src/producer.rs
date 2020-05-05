use std::fs::{File, OpenOptions};
use std::sync::mpsc;
use std::net::{TcpStream, Shutdown};
use std::io::{Read, Write, SeekFrom, Seek, ErrorKind};
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
            index : 0, //todo: read from last written + 1
            data_file : f_data,
            index_file : f_index,
            current_producer : None,
        }
    }

    fn write(&mut self, slice : &[u8]) -> usize {
        println!("content :_{}_", str::from_utf8(slice).expect("failed to format"));
        let written = self.data_file.write(slice).unwrap();
        let file_position = self.data_file.seek(SeekFrom::End(0)).unwrap();
        let file_position_bytes = (file_position as u64).to_le_bytes(); // get start instead of end position
        self.index_file.write( &file_position_bytes).unwrap();
        self.index += 1;
        written
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
    rec_upto : u32,
    tcp : TcpStream,
}
impl ClientBuff {
    fn new (stream : TcpStream, producer_id : u32) -> ClientBuff {
        ClientBuff {
            producer_id : producer_id,
            state : BufferState::Pending, 
            topic : None, 
            buffer : [0; BUFF_SIZE],
            buff_pos : 0, // position of end of buffer (read from tcp) 
            rec_size : 0, // size of record excluding 4 byte size
            rec_pos : 0,  // position of last byte processed in buffer
            rec_upto : 0, // no of bytes processed so far in record
            tcp : stream,
        }

    }

    fn read_rec_to(&self) -> usize {
        let buff_toread = self.buff_pos - self.rec_pos;
        let rec_toread = self.rec_size - self.rec_upto;

        if buff_toread <=  rec_toread {
            //read to end of buffered
            self.buff_pos as usize
        } else {
            //read just up to end of record
            (self.rec_pos + self.rec_size) as usize
        }
    }

    fn set_rec_size(&mut self) {
        if self.rec_size == 0 && self.buff_pos >= self.rec_pos + 4 {
            let start = self.rec_pos as usize;
            let end = self.rec_pos as usize + 4 ;
            self.rec_size = u32::from_le_bytes(self.buffer[start..end].try_into().expect("slice with incorrect length"));
            self.rec_pos += 4;
        }
    }

    fn read(&mut self) {
        match self.tcp.read(&mut self.buffer[self.buff_pos as usize..BUFF_SIZE]) {
            Ok(size) => {
                self.buff_pos += size as u32;
            },
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
            },
            Err(_e) => {
                panic!("Error trying to read into clientbuff"); 
            },
        }
    }

    fn reset(&mut self) {
        // if all buffer is read reset 
        if self.buff_pos == self.rec_pos {
            self.buff_pos = 0;
            self.rec_pos = 0;
        } else if self.buff_pos < self.rec_pos {
            panic!("record beyond end of buffer");
        }

        // if record is completely written reset
        if self.rec_upto == self.rec_size {
            self.rec_upto = 0;
            self.rec_size = 0;
        } else if self.rec_upto > self.rec_size {
            panic!("read past end of record? shouldn't happen...");
        }
    }

    fn process_data(&mut self, topic_list : &mut HashMap<String,Topic>) {
        println!("process data...");
        self.set_rec_size();

        println!("rec size set - buff_pos {}, rec_pos {}, rec_size {}, rec_upto {}", self.buff_pos, self.rec_pos, self.rec_size, self.rec_upto);

        if self.buff_pos >= self.rec_pos && self.rec_size > 0 {  // has unread data and size has been set ok
            match &self.topic {
                Some(topic_name) => match topic_list.get_mut(topic_name) {
                    Some(topic) => {
                        //let written = topic.write(&self);
                        let slice = &self.buffer[self.rec_pos as usize .. self.read_rec_to()];
                        let written = topic.write(slice);

                        self.rec_pos += written as u32;
                        self.rec_upto += written as u32;
                        
                        self.reset();

                        if self.rec_size == 0 {
                            topic.current_producer = None;
                            topic.index += 1;
                        }

                        println!("adding {} to rec_pos to get {} ", written, self.rec_pos);
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

        self.read();
    }

    fn validate_token(&mut self) {
        println!("validating token");

        let start = self.rec_pos as usize;
        let end = (self.rec_pos + self.rec_size) as usize;
      
        let st = str::from_utf8(&self.buffer[start..end]).unwrap();

        for x in st.chars() {
            println!("{}", x);
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

        self.read();
        self.set_rec_size();

        println!("rec_size :{}",self.rec_size);
        println!("buff_pos :{}",self.buff_pos);
        println!("rec_pos :{}",self.rec_pos);

        if self.rec_size > 0 && self.buff_pos >= self.rec_size + self.rec_pos {
            self.validate_token();
            self.rec_pos += self.rec_size;
            self.rec_upto += self.rec_size;
            self.reset();
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

