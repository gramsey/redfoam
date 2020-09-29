use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Write, Read, SeekFrom, Seek};
use inotify::{Inotify, WatchMask, WatchDescriptor };
use std::collections::HashMap;
use std::convert::TryInto;

use super::er::Er;
use super::trace;
use super::config::{Config, TopicConfig};

pub struct Topic {
    index : u64,
    data_file : File,  /* there are the 'current' files only */
    index_file : File, 
    current_producer : Option<u32>,
    pub last_data_offset : u64,
    pub last_index_offset : u64,
    is_producer : bool,
    config : TopicConfig,
}
impl Topic {

    pub fn open (config : TopicConfig, is_producer : bool) -> Result<Topic, Er>  {

        let mut f_data  = Topic::latest_file(is_producer, 'd', &config)?;
        let mut f_index = Topic::latest_file(is_producer, 'i', &config)?;

        let last_index = f_index.seek(SeekFrom::End(0)).unwrap(); 
        let last_data = f_data.seek(SeekFrom::End(0)).unwrap(); 
        let idx = last_index / 8;

        Ok(Topic {
            index : idx,
            data_file : f_data,
            index_file : f_index,
            current_producer : None,
            last_data_offset : last_data,
            last_index_offset : last_index,
            is_producer : is_producer,
            config : config,
        })
    }


    fn latest_file(is_producer : bool, prefix : char, config : &TopicConfig) -> Result<File, Er> {

        let file_number = Topic::latest_file_number(&config.topic_name, &config.folder)?;
        let file_name = format!("{}/{}/{}{:016x}", config.folder, config.topic_name, prefix, file_number);

        Self::file_opener(is_producer).open(&file_name)
            .map_err(|e| Er::CantOpenFile(e))
    }

    fn latest_file_number(topic_name : &String, folder : &String) -> Result<u64, Er> {
        let topic_folder = format!("{}/{}", folder, topic_name);

        let mut latest_file_number: u64 = 0;

        for entry in fs::read_dir(topic_folder)
                        .map_err(|e| Er::CantReadDir(e))? {

            let file = entry
                .map_err(|e| Er::CantReadFile(e))?;

            let file_name = file.file_name();

            let f_name = file_name.to_str()
                .ok_or(Er::BadFileName)?;

            let first_char = f_name.chars().nth(0)
                .ok_or(Er::BadFileName)?;

            if first_char == 'i' {
                let file_number = u64::from_str_radix(&f_name[1..], 16)
                    .map_err(|e| Er::BadOffset(String::from(f_name), e))?;

                if file_number > latest_file_number { 
                    latest_file_number = file_number
                }
            }
        }
        Ok(latest_file_number)
    }


    fn file_opener(is_producer : bool) -> OpenOptions {
        let mut f_options = OpenOptions::new();
        if is_producer { f_options.append(true).create(true); } else { f_options.read(true); }
        f_options
    }

    fn file_position(&self, record_index : u64) -> usize {
        let records_per_file = 2^(self.config.file_mask as u64 * 4);
        (record_index % records_per_file) as usize
    }

    fn file_number(&self, record_index : u64) -> usize {
        let records_per_file = 2^(self.config.file_mask as u64 * 4);
        (record_index % records_per_file) as usize
    }

    fn data_file_at(&mut self, offset: usize) -> &File {
        unimplemented!();
        let mut result = &self.data_file;
        result
    }

    fn index_file_at(&mut self, offset: usize) -> &File {
        unimplemented!();
        let mut result = &self.index_file;
        result
    }

    pub fn write(&mut self, slice : &[u8]) -> Result<usize, Er> {
        let written = self.data_file.write(slice)
            .map_err(|e| Er::CantWriteFile(e))?;
        Ok(written)
    }

    pub fn end_rec(&mut self) -> Result<u64, Er> {
        let idx = self.index;
        self.index += 1;

        let file_position = self.data_file.seek(SeekFrom::End(0))
            .map_err(|e| Er::CantReadFile(e))?;

        let file_position_bytes = (file_position as u64).to_le_bytes();

        self.index_file.write( &file_position_bytes)
            .map_err(|e| Er::CantWriteFile(e))?;

        self.current_producer = None;
        self.create_file_check()?;
        Ok(idx)
    }

    // only called by producers
    fn create_file_check (&mut self) -> Result<(), Er> {
        if self.file_position(self.index) == 0 {
            let num = self.file_number(self.index);

            self.data_file = Self::file_opener(true).open(format!("d{}", num))
                .map_err(|e| Er::CantOpenFile(e))?;

            self.index_file = Self::file_opener(true).open(format!("i{}", num))
                .map_err(|e| Er::CantOpenFile(e))?;
        }
        Ok(())
    }

    // only called by consumers
    pub fn switch_file(&mut self, file_name: &str) -> Result<(), Er> {
        let file = Self::file_opener(false).open(file_name)
            .map_err(|e| Er::CantOpenFile(e))?;

        match file_name.chars().nth(0).ok_or(Er::BadFileName)? {
            'i' => self.index_file = file,
            'd' => self.data_file = file,
            _ =>  return Err(Er::BadFileName),
        }
        Ok(())
    }

    pub fn read_index_into(&mut self, buf : &mut [u8], start : u64) -> Result<usize,Er> {

        self.index_file.seek(SeekFrom::Start(start))
            .map_err(|e| Er::CantReadFile(e))?;

        match self.index_file.read(buf) {
            Ok(size) => { 
                trace!("read_index_into() : start : {}, size {}", start, size); 
                Ok(size)
            },
            Err(e) => { Err(Er::CantReadFile(e)) },
        }
    }

    pub fn read_data_into(&mut self, buf : &mut [u8], start : u64) -> Result<usize,Er> {

        self.data_file.seek(SeekFrom::Start(start))
            .map_err(|e| Er::CantReadFile(e))?;

        match self.data_file.read(buf) {
            Ok(size) => { 
                trace!("read_data_into() : start : {}, size {}", start, size); 
                Ok(size) 
            },
            Err(e) => { Err(Er::CantReadFile(e)) },
        }
    }

    pub fn read_index_latest(&mut self, buf : &mut [u8]) -> Result<(u64, usize),Er> {
        let mut size = self.read_index_into(buf, self.last_index_offset)?; 
        size = size - (size % 8); /* should be a NOP - but dont want to worry about partial read */
        let result = (self.last_index_offset, size);
        self.last_index_offset += size as u64;
        Ok(result)
    }

    pub fn read_data_latest(&mut self, buf : &mut [u8]) -> Result<(u64, usize),Er> {
        println!("read data latest");
        let size = self.read_data_into(buf, self.last_data_offset)?; 
        let result = (self.last_data_offset, size);
        self.last_data_offset += size as u64;
        Ok(result)
    }

    fn _read_index(&mut self, rec_no: u64) -> Result<(u64, u64), Er> {
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

        self.index_file.seek(SeekFrom::Start(position)).unwrap();
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
    topic_names : HashMap<String, u32>,
    topics : HashMap<u32, Topic>,
    pub followers : HashMap<u32, Vec<u32>>,
    pub watchers : HashMap<WatchDescriptor, u32>,
    pub notify : Inotify,
}
impl TopicList {

    pub fn init (is_producer : bool) -> Result<TopicList, Er> {

        let topic_names : HashMap<String, u32> = HashMap::new();
        let topics : HashMap<u32, Topic> = HashMap::new();
        let watchers : HashMap<WatchDescriptor, u32> = HashMap::new();
        let followers : HashMap<u32, Vec<u32>> = HashMap::new();
        let notify = Inotify::init().expect("Inotify initialization failed - does this linux kernel support inotify?");
        let mut config = Config::new();

        let mut topic_list = TopicList {
            topic_names,
            topics,
            followers,
            watchers,
            notify,
        };

        while let Some(topic_cfg) = config.topics.pop() {
            topic_list.add_topic(topic_cfg, is_producer);
        }

        Ok(topic_list)
    }

    fn add_topic(&mut self, topic_cfg : TopicConfig, is_producer : bool) -> Result<(),Er>{

        let t = Topic::open(topic_cfg, is_producer)?;

        if !is_producer {
            let folder = format!("{}/{}", t.config.folder, t.config.topic_name);
            let wd = self.notify.add_watch(folder, WatchMask::MODIFY | WatchMask::CREATE)
                .map_err(|e| Er::InotifyError(e))?;
            self.watchers.insert(wd, t.config.topic_id);
        }

        self.topic_names.insert(t.config.topic_name.clone(), t.config.topic_id);
        self.topics.insert(t.config.topic_id, t);

        Ok(())
    }

    pub fn get_topic(&self, name : &String) -> u32 {
        *self.topic_names.get(name).unwrap()
    }
    pub fn topic_for_id(&mut self, topic_id : u32) -> Result<&mut Topic, Er> {
        let topic = self.topics.get_mut(&topic_id).ok_or(Er::TopicNotFound)?;
        Ok(topic)
    }
/*
    pub fn get_topics(&self, _filter : &str) -> Result<Option<Vec<u16>>,Er> {
        let mut x : Vec<u16> = Vec::new();
        x.push(1);
        Ok(Some(x))
    }
    */
    pub fn follow_topic(&mut self, topic_id : u32, client_id: u32) -> Result<(u64, u64), Er> {
        println!("following {}", topic_id);
        match self.followers.get_mut(&topic_id) {
            Some(clients) => clients.push(client_id),
            None => {
                let mut client_list : Vec<u32> = Vec::new();
                client_list.push(client_id);
                println!("created client list for topic {}", topic_id);
                self.followers.insert(topic_id, client_list);
            },
        }
        if let Some(topic) = self.topics.get_mut(&topic_id) {
            Ok((topic.last_index_offset, topic.last_data_offset))
        } else {
            Err(Er::TopicNotFound)
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_file() {
        fs::create_dir("/tmp/testx");
        File::create("/tmp/testx/d0000000000000000");
        File::create("/tmp/testx/i0000000000000000");
        File::create("/tmp/testx/d000000000000001c");
        File::create("/tmp/testx/i000000000000001c");
        File::create("/tmp/testx/d00000000000000a3");
        File::create("/tmp/testx/i00000000000000a3");

        let file_number = Topic::latest_file_number(&String::from("testx"), &String::from("/tmp")).expect("file_numb");
        assert_eq!(file_number, 0xa3);

        let config = TopicConfig {
            topic_id : 1,
            topic_name : String::from("testx"),
            folder : String::from("/tmp"),
            replication : 0, 
            file_mask : 8,
        };

        let dp_latest_file_result = Topic::latest_file(true, 'd', &config);
        assert!(dp_latest_file_result.is_ok(), "trying to get latest data file for writing");

        let dp_latest_metadata_result = dp_latest_file_result.unwrap().metadata();
        assert!(dp_latest_metadata_result.is_ok(), "trying to get latest data file for writing (metadata)");

        let dp_latest_metadata = dp_latest_metadata_result.unwrap();
        assert!(dp_latest_metadata.is_file(), "checking file object is real file (not directly or sym link");
    }

    #[test]
    fn test_topicwrite() {
        let topic_config = TopicConfig {
            topic_id : 1,
            topic_name : String::from("test"),
            folder : String::from("/tmp"),
            replication : 0,
            file_mask : 0,
        };

        let mut t = Topic::open(topic_config, true).expect("trying to create topic");
        let idx = t.index;
        let written : usize = t.write(&b"hello"[..]).expect("trying to write to file");
        assert_eq!(written, 5, "5 bytes should be written to file");
        t.end_rec();
        assert_eq!(t.index, idx + 1, "checking index is incremented by one");
    }

    #[test]
    fn test_readfirstrecord() {
        let topic_config = TopicConfig {
            topic_id : 1,
            topic_name : String::from("test"),
            folder : String::from("/tmp"),
            replication : 0,
            file_mask : 0,
        };

        let mut t = Topic::open(topic_config, false).expect("trying to create topic");
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
        let topic_config = TopicConfig {
            topic_id : 1,
            topic_name : String::from("test"),
            folder : String::from("/tmp"),
            replication : 0,
            file_mask : 0,
        };

        let mut t = Topic::open(topic_config, false).expect("trying to create topic");
        match t._read_index(1) {
            Ok((position, size)) => {
                assert_eq!(position, 5);
                assert_eq!(size, 12);
            }
            Err(e) => assert!(false, "error on read index {}", e),
        }

        match t._read_index(0) {
            Ok((position, size)) => {
                assert_eq!(position, 0);
                assert_eq!(size, 5);
            }
            Err(e) => assert!(false, "error on read index {}", e),
        }
    }

    #[test]
    fn test_topiclist() {
        let mut tl_producer = TopicList::init(true).unwrap();
        let mut tl_consumer = TopicList::init(false).unwrap();

        let p_topic_result = tl_producer.topic_for_id(1);

        assert!(p_topic_result.is_ok(), "topic 1 should be in topic list");
        let p_topic = p_topic_result.unwrap();

        let write_result = p_topic.write(&b"TopicList test message"[..]);
        assert!(write_result.is_ok(),  "problem while writing to topic");

        assert_eq!(write_result.unwrap(), 22 as usize);

        let end_rec_result = p_topic.end_rec();
        assert!(end_rec_result.is_ok(),  "problem while writing to topic for end record");

        let mut buffer = [0; 1024];
        let mut events = tl_consumer.notify.read_events(&mut buffer)
            .expect("Error while reading events");

        /* first event for writing to data file */
        match events.next() {
            Some(e) => {
                match tl_consumer.watchers.get(&e.wd) {
                    Some(topic_id) => assert_eq!(*topic_id, 1u32),
                    None => assert!(false, "topic id missing from watcher list"),
                }
                match e.name {
                    Some(name) => {
                        assert_eq!(name.to_str().unwrap().chars().nth(0), Some('d'));
                    }, 
                    None => assert!(false, "event should have a file name"),
                }
            },
            None => assert!(false, "missing event from inotify"),
        }

        /* second event for writing to index file */
        match events.next() {
            Some(e) => {
                match tl_consumer.watchers.get(&e.wd) {
                    Some(topic_id) => assert_eq!(*topic_id, 1u32),
                    None => assert!(false, "topic id missing from watcher list"),
                }
                match e.name {
                    Some(name) => {
                        assert_eq!(name.to_str().unwrap().chars().nth(0), Some('i'));
                    }, 
                    None => assert!(false, "event should have a file name"),
                }
            },
            None => assert!(false, "missing event from inotify"),
        }
    }
}

