use std::fs::{File, OpenOptions};
use std::io::{Write, SeekFrom, Seek};
use std::str;
use std::collections::HashMap;

use super::buff::Buff;
use super::er::Er;


pub struct Topic {
    index : u64,
    data_file_write : File,
    index_file_write : File,
    data_file_read : File,
    index_file_read : File,
    current_producer : Option<u32>,
    data_buff : Buff,
    index_buff : Buff,
    pub data_offset : u64,
    pub index_offset : u64,
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
            data_buff : b_data,
            index_buff : b_index,
            data_offset : 0,
            index_offset : 0,
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

    pub fn read_index(&mut self, seq : u8) -> Result<Option<&[u8]>,Er> {
        if seq == self.index_buff.seq + 1 { 
            self.index_offset = self.index_file_read.seek(SeekFrom::Current(0)).map_err(|e| Er::CantReadFile(e))?;
            let size = self.index_buff.read_data(&mut self.index_file_read)?;
            self.index_buff.rec_size = Some(size as u32);               //this cast should never fail (usize -> u32) because read_data is constrained by buffer size 
            self.index_buff.seq = seq;
        }

        if self.index_buff.seq != seq { return Err(Er::InvalidSequence) }

        if self.index_buff.rec_size == Some(0) {
            Ok(None)
        } else { 
            Ok(Some(self.index_buff.data()))
        }
    }

    pub fn read_data(&mut self, seq : u8) -> Result<Option<&[u8]>,Er> {
        if seq == self.data_buff.seq + 1 { 
            self.data_offset = self.data_file_read.seek(SeekFrom::Current(0)).map_err(|e| Er::CantReadFile(e))?;
            let size = self.data_buff.read_data(&mut self.data_file_read)?;
            self.data_buff.rec_size = Some(size as u32);
            self.data_buff.seq = seq;
        }

        if self.data_buff.seq != seq { return Err(Er::InvalidSequence) }

        if self.data_buff.rec_size == Some(0) {
            Ok(None)
        } else { 
            Ok(Some(self.data_buff.data()))
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
    fn test_topicreadindex() {
        let mut t = Topic::new();

        match t.read_index(0) {
            Ok(d) => assert!(d.is_none()), 
            Err(e) => assert!(false, "failed with error {}", e),
        }

        match t.read_index(1) {
            Ok(d) => {
                match d {
                    Some(x) => assert_eq!(x.len() % 8, 0, "should be multiple of 8 bytes (u64)"),
                    None => assert!(false, "index file should have something in it"),
                }
            },
            Err(e) => assert!(false, "failed with error {}", e),
        }

    }
}

