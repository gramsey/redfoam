use std::fs::{File, OpenOptions};
use std::io::{Write, SeekFrom, Seek};
use std::str;
use std::collections::HashMap;

use super::buff::Buff;
use super::er::Er;


pub struct Topic {
    index : u64,
    data_file : File,
    index_file : File,
    current_producer : Option<u32>,
    data_buff : Option<Buff>,
    index_buff : Option<Buff>,
    pub data_offset : u64,
    pub index_offset : u64,
}
impl Topic {
    // new handle, topic must already exist as a sym link
    pub fn new () -> Topic  {
        let data_fname = format!("/tmp/{}/data", "test");
        let index_fname = format!("/tmp/{}/index", "test");

        let f_data = OpenOptions::new().append(true).open(data_fname).expect("cant open topic data file");
        let f_index = OpenOptions::new().append(true).open(index_fname).expect("cant open topic index file");

        Topic {
            index : 0, //todo: read from last written + 1
            data_file : f_data,
            index_file : f_index,
            current_producer : None,
            data_buff : None,
            index_buff : None,
            data_offset : 0,
            index_offset : 0,
        }
    }

    pub fn write(&mut self, slice : &[u8]) -> usize {
        println!("content :_{}_", str::from_utf8(slice).expect("failed to format"));
        let written = self.data_file.write(slice).unwrap();
        written
    }

    pub fn end_rec(&mut self) -> u64 {
        let idx = self.index;
        self.index += 1;

        let file_position = self.data_file.seek(SeekFrom::End(0)).unwrap();
        let file_position_bytes = (file_position as u64).to_le_bytes(); // get start instead of end position
        self.index_file.write( &file_position_bytes).unwrap();
        self.current_producer = None;

        idx
    }

    pub fn read_index(&mut self, seq : u8) -> Result<Option<&[u8]>,Er> {
        let buff = self.index_buff.get_or_insert(Buff::new());
        if seq == buff.seq {
            Ok(Some(buff.data()))
        } else {
            if seq == buff.seq + 1 {
                self.index_offset = self.index_file.seek(SeekFrom::Current(0)).map_err(|e| Er::CantReadFile(e))?;
                let size = buff.read_data(&mut self.index_file)?;
                if size == 0 {
                    Ok(None)
                } else { 
                    Ok(Some(buff.data()))
                }
            } else {
                Err(Er::InvalidSequence)
            }
        }
    }
    pub fn read_data(&mut self, seq : u8) -> Result<Option<&[u8]>,Er> {
        let buff = self.data_buff.get_or_insert(Buff::new());
        if seq == buff.seq {
            Ok(Some(buff.data()))
        } else {
            if seq == buff.seq + 1 {
                self.data_offset = self.data_file.seek(SeekFrom::Current(0)).map_err(|e| Er::CantReadFile(e))?;
                let size = buff.read_data(&mut self.data_file)?;
                if size == 0 {
                    Ok(None)
                } else { 
                    Ok(Some(buff.data()))
                }
            } else {
                Err(Er::InvalidSequence)
            }
        }
    }
/*
    pub fn read_data(&mut self, seq : u8) -> Result<Option<&[u8]>,Er> {
        self.data_buff.get_or_insert(Buff::new());
        read_file(self.data_file, &mut self.data_buff.unwrap(), seq)
    }
    */


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
