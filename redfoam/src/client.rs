use std::io::{Write};
use std::net::TcpStream;
use std::collections::VecDeque;
use std::str;
use std::sync::mpsc;
use std::thread;

use super::buff::Buff;
use super::tcp::{RecordType};
use super::er::Er;

pub struct ReadClient {
    io : TcpStream,
    seq : u8,
    tcp_buff : Buff,
    out : mpsc::Sender<Vec<u8>>,
}

impl ReadClient {
    pub fn new (tcp : TcpStream, out : mpsc::Sender<Vec<u8>>) -> ReadClient {

        ReadClient {
            io : tcp,
            seq : 0,
            tcp_buff : Buff::new(),
            out : out,
        }

    }

    pub fn run(&mut self) -> Result<(), Er> {
        let mut messages = Messages::new(0, 0);
        let mut record_type: Option<RecordType> = None;
        loop {

            // read new data off tcp queue
            self.tcp_buff.read_data(&mut self.io)?;
            if self.tcp_buff.rec_size.is_none() { self.tcp_buff.rec_size = self.tcp_buff.read_u32(); }
            self.tcp_buff.check_seq()?;
            if record_type.is_none() { record_type = self.tcp_buff.read_u8().map(|r| r.into()) }

            match record_type {
                Some(RecordType::DataFeed) => {
                    /* end_of_rec here can be a partial message - it refers to packet */
                    if self.tcp_buff.is_end_of_record() {
                        messages.push_data(self.tcp_buff.data());
                        record_type = None;
                    }
                },

                Some(RecordType::IndexFeed) => {
                    if let Some(idx) = self.tcp_buff.read_u64() {
                        messages.push_index(idx);
                        record_type = None;
                    }
                },

                None => { break;},
                _ => {},
            }

            // write any new whole messages to output
            for m in &mut messages {
                if let Err(e) = self.out.send(m) {
                    return Err(Er::FailedToReturnMessage(e))
                }
            }
        }
        Ok(())
    }
}

pub fn getReciever (topics : String, url : String, auth : String) -> std::io::Result<mpsc::Receiver<Vec<u8>>> {
    let (tx, rx) : (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();
    let mut tcp = TcpStream::connect(url)?;

    let crap = "test";
    let message = format!("{};{}",crap, auth);
    
    let size = message.len() as u32;
    let mess_type : u8 = 1; // 1 = auth
    let seq : u8 = 0;

    tcp.write(&size.to_le_bytes())?;
    tcp.write(&[seq])?;
    tcp.write(&[mess_type])?;
    tcp.write(message.as_bytes())?;


    thread::spawn(move || {
        ReadClient::new(tcp, tx).run();
    });

    Ok(rx)
}

pub struct Client {
    io : TcpStream,
    seq : u8,
}
impl Client {
    pub fn new (topic : String, url : String, auth : String) -> std::io::Result<Client> {

        let mut stream = TcpStream::connect(url)?;
        let message = format!("{};{}",topic, auth);
        
        let size = message.len() as u32;
        let mess_type : u8 = 1; // 1 = auth
        let seq : u8 = 0;

        stream.write(&size.to_le_bytes())?;
        stream.write(&[seq])?;
        stream.write(&[mess_type])?;
        stream.write(message.as_bytes())?;

        Ok (Client { io : stream, seq : seq + 1 })
    }

    pub fn send(&mut self, content : String) -> std::io::Result<()> {

        let len : u32 = content.len() as u32;
        let mess_type : u8 = 2; // 1 = producer
        let topic_id : u16 = 1;

        self.io.write(&len.to_le_bytes())?;
        self.io.write(&[self.seq])?;
        self.io.write(&[mess_type])?;
        self.io.write(&topic_id.to_le_bytes())?;

        self.io.write(content.as_bytes())?;
        self.seq = self.seq % 255 + 1;

        Ok(())
    }
}


pub struct Messages {
    data : VecDeque<u8>,
    index : VecDeque<u64>,
    data_offset : u64,
    index_offset : u64,
    last_index : Option<u64>,
}


impl Messages {
    fn new(data_offset : u64, index_offset : u64) -> Messages {
        Messages {
            data : VecDeque::new(),
            index : VecDeque::new(),
            data_offset : data_offset,
            index_offset : index_offset,
            last_index : None,
        }
    }

    fn push_data(&mut self, input_data: &[u8]) {
        for c in input_data {
            self.data.push_back(*c);
            self.data_offset += 1;
        }
    }

    fn push_index(&mut self, input_index: u64) {
        self.index.push_back(input_index);
        self.index_offset += 1;
    }
}

impl Iterator for Messages {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {

        if let Some(idx) = self.index.pop_front() {
            if let Some(old_idx) = self.last_index {

                let size = (idx - old_idx) as usize;

                let data = self.data.drain(..size).collect::<Vec<_>>();

                self.last_index = Some(idx);
                println!("vec {}", str::from_utf8(data.as_slice()).unwrap());
                Some(data)
            } else {
                println!("marker 1 idx: {}, data_offset : {}, len: {}", idx, self.data_offset, self.data.len());
                // first read, get rid of any data before first index
                let diff = (idx - (self.data_offset - self.data.len() as u64)) as usize;
                println!("marker 2 diff {}", diff);

                if diff > 0 {
                    self.data.drain(..diff);
                }
                
                self.last_index = Some(idx);
                None
            }
        } else { None } // no indexes to read
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue () {
        let message1 = b"This is the message";
        let message2 = b"Wow This is the second message";
        let message2a = &message2[..5];
        let message2b = &message2[5..];

        let mut q = Messages::new(100, 8);
        q.push_data(b"0123456789");
        q.push_data(message1);
        q.push_data(message2a);
        q.push_data(message2b);
        q.push_data(b"Some trailing junk...");

        let mut i = 100 + 10;
        q.push_index(i);
        i += message1.len() as u64;
        q.push_index(i);
        i += message2.len() as u64;
        q.push_index(i);

        let result1 = message1.to_vec();
        let result2 = message2.to_vec();
        
        assert_eq!(q.next(), None);
        assert_eq!(q.next(), Some(result1));
        assert_eq!(q.next(), Some(result2));
        assert_eq!(q.next(), None);
    }
}
