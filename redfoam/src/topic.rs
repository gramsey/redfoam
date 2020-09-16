use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Write, Read, SeekFrom, Seek};
use inotify::{Inotify, WatchMask, WatchDescriptor };
use std::str;
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
    file_mask : u8,
}
impl Topic {

    pub fn open (topic_name : &String, folder : &String, replication : u8, file_mask : u8, is_producer : bool) 
    -> Result<Topic, Er>  {

        let file_number = Topic::latest_file_number(topic_name, folder)?;
        println!("file_number : {} ", file_number);

        let data_fname = format!("{}/{}/d{:016x}",folder, topic_name, file_number);
        let index_fname = format!("{}/{}/i{:016x}", folder, topic_name, file_number);

        println!("data_fname {} index_fname {}", data_fname, index_fname);

        let mut f_options = OpenOptions::new();

        if is_producer { f_options.append(true); } else { f_options.read(true); }

        let mut f_data = f_options.open(&data_fname).expect("cant open topic data file");
        let mut f_index = f_options.open(&index_fname).expect("cant open topic data file");

        let last_index = f_index.seek(SeekFrom::End(0)).unwrap(); 
        let last_data = f_data.seek(SeekFrom::End(0)).unwrap(); 
        let idx = last_index / 8;


        trace!("creating topic name : {}  idx : {}, data file : {}, index file : {} last index offset : {} last data offset : {}",topic_name, idx, data_fname, index_fname, last_index, last_data);   

        Ok(Topic {
            index : idx,
            data_file : f_data,
            index_file : f_index,
            current_producer : None,
            last_data_offset : last_data,
            last_index_offset : last_index,
            is_producer : is_producer,
            file_mask : file_mask,
        })
    }

    fn latest_file_number(topic_name : &String, folder : &String) -> Result<u64, Er> {
        let topic_folder = format!("{}/{}", folder, topic_name);

        let mut latest_file_number: u64 = 0;
        match fs::read_dir(topic_folder) {
            Ok(files) => {
                for f in files {
                    match f {
                        Ok(f) => {
                            if let Some(f_name) = f.file_name().to_str() {
                                let first_char = f_name.chars().nth(0).ok_or(Er::IsNone)?;
                                if first_char == 'i' {
                                    match u64::from_str_radix(&f_name[1..], 16) {
                                        Ok(file_number) =>  {
                                            if file_number > latest_file_number { 
                                                latest_file_number = file_number
                                            }
                                        },
                                        Err(e)  => {
                                            return Err(Er::BadOffset(String::from(f_name), e))
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            unimplemented!();
                        }
                    }
                }
            },
            Err(_) => {
                unimplemented!();
            }
        }
        Ok(latest_file_number)
    }

    fn file_number(&self, offset : u64) -> String {
        let records_per_file = 2^(self.file_mask as u64 * 4);
        let file_number = offset / records_per_file; /* integer division, remainder is ignored */
        format!("{:16x}",file_number)
    }

    fn file_position(&self, offset : u64) -> usize {
        let records_per_file = 2^(self.file_mask as u64 * 4);
        (offset % records_per_file) as usize
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


    pub fn write(&mut self, slice : &[u8]) -> usize {
        println!("content :_{}_", str::from_utf8(slice).expect("failed to format"));
        let written = self.data_file.write(slice).unwrap();
        written
    }

    pub fn end_rec(&mut self) -> u64 {
        let idx = self.index;
        self.index += 1;

        let file_position = self.data_file.seek(SeekFrom::End(0)).unwrap();
        let file_position_bytes = (file_position as u64).to_le_bytes();
        self.index_file.write( &file_position_bytes).unwrap();
        self.current_producer = None;
        idx
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

    pub fn new (is_producer : bool) -> TopicList {

        let topic_names : HashMap<String, u32> = HashMap::new();
        let topics : HashMap<u32, Topic> = HashMap::new();
        let watchers : HashMap<WatchDescriptor, u32> = HashMap::new();
        let followers : HashMap<u32, Vec<u32>> = HashMap::new();
        let notify = Inotify::init().expect("Inotify initialization failed - does this linux kernel support inotify?");
        let config = Config::new();

        let mut topic_list = TopicList {
            topic_names,
            topics,
            followers,
            watchers,
            notify,
        };

        for topic_cfg in config.topics {
            topic_list.add_topic(
                topic_cfg.topic_id, 
                topic_cfg.topic_name, 
                topic_cfg.folder,
                topic_cfg.replication,
                topic_cfg.file_mask,
                is_producer);
        }
        topic_list
    }

    fn add_topic(&mut self, topic_id : u32, topic_name : String, topic_folder : String, replication : u8, file_mask : u8, is_producer : bool) -> Result<(),Er>{

        let t = Topic::open(&topic_name, &topic_folder, replication, file_mask, is_producer)?;

        if !is_producer {
            let folder = format!("/tmp/{}", topic_name);
            let wd = self.notify.add_watch(folder, WatchMask::MODIFY)
                .map_err(|e| Er::InotifyError(e))?;
            self.watchers.insert(wd, topic_id);
        }

        self.topics.insert(topic_id, t);
        self.topic_names.insert(topic_name, topic_id);

        Ok(())
    }

    pub fn get_topic(&self, name : &String) -> u32 {
        *self.topic_names.get(name).unwrap()
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


    pub fn write(&mut self, topic_id : u32, data : &[u8]) -> usize {
        let written = self.topics.get_mut(&topic_id).unwrap().write(data);
        written
    }

    pub fn read_index(&mut self, topic_id : u32, data : &mut [u8]) -> Result<(u64, usize),Er> {
        let result = self.topics.get_mut(&topic_id).unwrap().read_index_latest(data)?;
        Ok(result)
    }

    pub fn read_data(&mut self, topic_id : u32, data : &mut [u8]) -> Result<(u64, usize),Er> {
        let result = self.topics.get_mut(&topic_id).unwrap().read_data_latest(data)?;
        Ok(result)
    }
    pub fn end_record(&mut self, topic_id : u32) -> u64 {
        self.topics.get_mut(&topic_id).unwrap().end_rec()
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
    }

    #[test]
    fn test_topicwrite() {
        let mut t = Topic::open(&String::from("test"), &String::from("/tmp"), 0, 0,  true).expect("trying to create topic");
        let idx = t.index;
        let written : usize = t.write(&b"hello"[..]);
        assert_eq!(written, 5, "5 bytes should be written to file");
        t.end_rec();
        assert_eq!(t.index, idx + 1, "checking index is incremented by one");
    }

    #[test]
    fn test_readfirstrecord() {
        let mut t = Topic::open(&String::from("test"), &String::from("/tmp"), 0, 0,  false).expect("trying to create topic");
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
        let mut t = Topic::open(&String::from("test"), &String::from("/tmp"), 0, 0,  false).expect("trying to create topic");
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
        let mut tl_producer = TopicList::new(true);
        let mut tl_consumer = TopicList::new(false);

        tl_producer.write(1,&b"TopicList test message"[..]);
        tl_producer.end_record(1);

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

