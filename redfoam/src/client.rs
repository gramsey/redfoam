use std::io::{Write};
use std::net::TcpStream;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;

use super::buff::Buff;
use super::tcp::{RecordType};
use super::er::Er;

pub struct ReadClient {
    io : TcpStream,
    _seq : u8,
    tcp_buff : Buff,
    out : mpsc::Sender<Vec<u8>>,
}

impl ReadClient {
    pub fn new (tcp : TcpStream, out : mpsc::Sender<Vec<u8>>) -> ReadClient {

        ReadClient {
            io : tcp,
            _seq : 0,
            tcp_buff : Buff::new(),
            out : out,
        }

    }

    pub fn run(&mut self) -> Result<(), Er> {
        let mut messages: Option<Messages> = None;
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
                        if let Some(mgs) = &mut messages {
                            (*mgs).push_data(self.tcp_buff.data());
                        } else {
                            return Err(Er::NoConsumerStart)
                        }
                    }
                },

                Some(RecordType::IndexFeed) => {
                    if let Some(idx) = self.tcp_buff.read_u64() {
                        if let Some(mgs) = &mut messages {
                            (*mgs).push_index(idx);
                            record_type = None;
                        } else {
                            return Err(Er::NoConsumerStart)
                        }
                    }
                },

                Some(RecordType::ConsumerStart) => {
                    if self.tcp_buff.is_end_of_record() {
                        let data_start;
                        let index_start;

                        if let Some(offset) = self.tcp_buff.read_u64() {
                            data_start = offset;
                        } else {
                            return Err(Er::FailedToReadDataStart);
                        }

                        if let Some(offset) = self.tcp_buff.read_u64() {
                            index_start = offset;
                        } else {
                            return Err(Er::FailedToReadDataStart);
                        }

                        messages = Some(Messages::new(data_start, index_start));
                        record_type = None;
                    }

                },

                None => { break;},
                _ => {},
            }

            // write any new whole messages to output
            if let Some(mess) = &mut messages {
                for m in &mut *mess {
                    if let Err(e) = self.out.send(m) {
                        return Err(Er::FailedToReturnMessage(e))
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn follow_topic(topic_id : u32, url : String, auth : String) -> std::io::Result<mpsc::Receiver<Vec<u8>>> {

    let (tx, rx) : (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();
    let mut tcp = TcpStream::connect(url)?;

    let mut size = auth.len() as u32;
    let mut mess_type : u8 = 1; // 1 = auth
    let mut seq : u8 = 0;

    tcp.write(&size.to_le_bytes())?;
    tcp.write(&[seq])?;
    tcp.write(&[mess_type])?;
    tcp.write(auth.as_bytes())?;

    seq = seq + 1;
    size = 5u32;
    mess_type = RecordType::ConsumerFollowTopics as u8; // 1 = auth

    tcp.write(&size.to_le_bytes())?;
    tcp.write(&[seq])?;
    tcp.write(&[mess_type])?;
    tcp.write(&topic_id.to_le_bytes())?;


    thread::spawn(move || {
        ReadClient::new(tcp, tx).run();
    });

    Ok(rx)
}
/*
pub fn get_reciever (_topics : String, url : String, auth : String) -> std::io::Result<mpsc::Receiver<Vec<u8>>> {
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
*/

pub struct Listener {
    client : Client,
    messages : Messages,
}

impl Listener {
    pub fn new (topic : String, url : String, auth : String) -> Result<Listener, Er> {
        let mut client = Client::new(topic, url, auth).expect("cant create client");
        let messages;
        client.follow_topic(1);

        match client.next()? {
            Some(RecordType::ConsumerFollowTopics) => {
                let index_offset = client.tcp_buff.read_u64().unwrap();
                let data_offset = client.tcp_buff.read_u64().unwrap();
                messages = Messages::new(index_offset, data_offset);
            }, 
            Some(_) => return Err(Er::FailedToReadDataStart),
            None => return Err(Er::IsNone),
        }
        client.set_blocking(false);
        Ok(Listener { client, messages })
    }

    pub fn next(&mut self) {
        match self.client.next().expect("cant read next from client") {
                Some(RecordType::DataFeed) => {},
                Some(RecordType::IndexFeed) => {},
                _ => {unimplemented!()}
        }
    }
}


pub struct Client {
    io : TcpStream,
    seq : u8,
    pub tcp_buff : Buff,
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

        Ok (Client { io : stream, seq : seq + 1 , tcp_buff : Buff::new() })
    }

    pub fn send(&mut self, content : String) -> std::io::Result<()> {

        let len : u32 = content.len() as u32;
        let mess_type : u8 = 2; // 1 = producer
        let topic_id : u32 = 1;

        self.io.write(&len.to_le_bytes())?;
        self.io.write(&[self.seq])?;
        self.io.write(&[mess_type])?;
        self.io.write(&topic_id.to_le_bytes())?;

        self.io.write(content.as_bytes())?;
        self.seq = self.seq % 255 + 1;

        Ok(())
    }

    pub fn follow_topic(&mut self, topic_id : u32) -> std::io::Result<()> {

        let len = 5u32;
        let mess_type : u8 = 3; // 1 = producer

        self.io.write(&len.to_le_bytes())?;
        self.io.write(&[self.seq])?;
        self.io.write(&[mess_type])?;
        self.io.write(&topic_id.to_le_bytes())?;

        self.seq = self.seq % 255 + 1;

        Ok(())
    }

    pub fn next(&mut self) -> Result<Option<RecordType>, Er> {
        self.tcp_buff.read_data(&mut self.io)?;
        if self.tcp_buff.rec_size.is_none() { self.tcp_buff.rec_size = self.tcp_buff.read_u32(); }

        if self.tcp_buff.is_end_of_record() {
            let record_type = self.tcp_buff.read_u8().map(|r| r.into());
            Ok(record_type)
        }
        else {
            Ok(None)
        }
    }

    pub fn set_blocking (&mut self, is_blocking : bool) {
        self.io.set_nonblocking(is_blocking).expect("set_nonblocking call failed");
    }

    pub fn data(&self) -> &[u8] { self.tcp_buff.data() }
    pub fn reset(&mut self) { self.tcp_buff.reset() }

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
                Some(data)
            } else {
                // first read, get rid of any data before first index
                let diff = (idx - (self.data_offset - self.data.len() as u64)) as usize;

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
