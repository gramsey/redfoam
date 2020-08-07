use std::fs::{File, OpenOptions};
use std::io::{Write, Read, SeekFrom, Seek};
use std::str;
use std::collections::HashMap;
use std::convert::TryInto;

use super::buff::Buff;
use super::er::Er;


pub struct Topic {
    index : u64,
    data_file_write : File,
    index_file_write : File,
    data_file_read : File,
    index_file_read : File,
    current_producer : Option<u32>,
    pub last_data_offset : u64,
    pub last_index_offset : u64,
}
impl Topic {
    // new handle, topic must already exist as a sym link
    pub fn new () -> Topic  {
        let data_fname = format!("/tmp/{}/data", "test");
        let index_fname = format!("/tmp/{}/index", "test");

        let f_data_write = OpenOptions::new().append(true).open(&data_fname).expect("cant open topic data file for writing");
        let mut f_index_write = OpenOptions::new().append(true).open(&index_fname).expect("cant open topic index file for writing");
        let f_data_read = OpenOptions::new().read(true).open(&data_fname).expect("cant open topic data file for reading");
        let f_index_read = OpenOptions::new().read(true).open(&index_fname).expect("cant open topic index file for reading");

        let idx = f_index_write.seek(SeekFrom::End(0)).unwrap() / 8;

        let mut b_data = Buff::new();
        b_data.rec_size=Some(0);

        let mut b_index = Buff::new();
        b_index.rec_size=Some(0);

        Topic {
            index : idx,
            data_file_write : f_data_write,
            index_file_write : f_index_write,
            data_file_read : f_data_read,
            index_file_read : f_index_read,
            current_producer : None,
            last_data_offset : 0,
            last_index_offset : 0,
        }
    }

    pub fn write(&mut self, slice : &[u8]) -> usize {
        println!("content :_{}_", str::from_utf8(slice).expect("failed to format"));
        let written = self.data_file_write.write(slice).unwrap();
        written
    }

    pub fn end_rec(&mut self) -> u64 {
        let idx = self.index;
        self.index += 1;

        let file_position = self.data_file_write.seek(SeekFrom::End(0)).unwrap();
        let file_position_bytes = (file_position as u64).to_le_bytes(); // get start instead of end position
        self.index_file_write.write( &file_position_bytes).unwrap();
        self.current_producer = None;

        idx
    }

    pub fn read_index_into(&mut self, buf : &mut [u8], start : u64) -> Result<usize,Er> {

        self.last_index_offset = self.index_file_read.seek(SeekFrom::Start(start)).map_err(|e| Er::CantReadFile(e))?;

        match self.index_file_read.read(buf) {
            Ok(size) => { Ok(size) },
            Err(e) => { Err(Er::CantReadFile(e)) },
        }
    }

    pub fn read_data_into(&mut self, buf : &mut [u8], start : u64) -> Result<usize,Er> {

        self.last_index_offset = self.data_file_read.seek(SeekFrom::Start(start)).map_err(|e| Er::CantReadFile(e))?;

        match self.data_file_read.read(buf) {
            Ok(size) => { Ok(size) },
            Err(e) => { Err(Er::CantReadFile(e)) },
        }
    }

    fn read_index(&mut self, rec_no: u64) -> Result<(u64, u64), Er> {
        let mut buf : [u8; 16] = [0;16];
        let position : u64;
        let idx_size : u64;

        // todo: if  rec_no % filesize == 0
        if rec_no == 0 { 
            idx_size = 8;
            position = 0;
        } else { 
            idx_size = 16; 
            position = (rec_no - 1) * 8;
        }

        let idx = self.index_file_read.seek(SeekFrom::Start(position)).unwrap();
        self.read_index_into(&mut buf[..idx_size as usize], position)?;

        let start : u64;
        let data_size;
        if rec_no == 0 { 
            data_size = u64::from_le_bytes(buf[..8].try_into().expect("should always be 8 bytes -> u64"));
            Ok((0, data_size))
        } else { 
            start = u64::from_le_bytes(buf[..8].try_into().expect("should always be 8 bytes -> u64"));
            let end = u64::from_le_bytes(buf[8..16].try_into().expect("should always be 8 bytes -> u64"));
            Ok((start, end - start))
        }
    }
}

pub struct TopicList {
    topic_names : HashMap<String, u16>,
    topics : HashMap<u16, Topic>,
}
impl TopicList {

    pub fn new () -> TopicList {
        println!("creating topic list");
        let mut topic_names : HashMap<String, u16> = HashMap::new();
        let mut topics : HashMap<u16, Topic> = HashMap::new();

        topics.insert(1, Topic::new());
        topic_names.insert(String::from("test"), 1);

        TopicList {
            topic_names,
            topics,
        }
    }

    pub fn get_topic(&self, name : &String) -> u16 {
        *self.topic_names.get(name).unwrap()
    }

    pub fn get_topics(&self, _filter : &str) -> Result<Option<Vec<u16>>,Er> {
        let mut x : Vec<u16> = Vec::new();
        x.push(1);
        Ok(Some(x))
    }

    pub fn write(&mut self, topic_id : u16, data : &[u8]) -> usize {
        let written = self.topics.get_mut(&topic_id).unwrap().write(data);
        println!(" wrote {}", written);
        written
    }

    pub fn end_record(&mut self, topic_id : u16) -> u64 {
        self.topics.get_mut(&topic_id).unwrap().end_rec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topicwrite() {
        let mut t = Topic::new();
        let idx = t.index;
        let written : usize = t.write(&b"hello"[..]);
        assert_eq!(written, 5, "5 bytes should be written to file");
        t.end_rec();
        assert_eq!(t.index, idx + 1, "checking index is incremented by one");
    }

    #[test]
    fn test_readfirstrecord() {
        let mut t = Topic::new();
        let mut buf : [u8; 8] = [0;8];

        match t.read_index_into(&mut buf, 0) {
            Err(e) => assert!(false, "error reading index {}", e),
            Ok(n) => assert_eq!(n, 8, "check 8 index bytes read"),
        }

        let idx = u64::from_le_bytes(buf);

        assert_ne!(idx, 0, "check index value is not zero");

        match t.read_data_into(&mut buf[..idx as usize], 0) {
            Err(e) => assert!(false, "error reading data {}", e),
            Ok(n) => assert_eq!(n, idx as usize, "check {} data bytes read", n),
        }

        
    }

    #[test]
    fn test_readindex() {
        let mut t = Topic::new();
        match t.read_index(1) {
            Ok((position, size)) => {
                assert_eq!(position, 5);
                assert_eq!(size, 12);
            }
            Err(e) => assert!(false, "error on read index {}", e),
        }

        match t.read_index(0) {
            Ok((position, size)) => {
                assert_eq!(position, 0);
                assert_eq!(size, 5);
            }
            Err(e) => assert!(false, "error on read index {}", e),
        }
    }

}

