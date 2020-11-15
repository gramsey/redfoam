use std::fs;
use std::fs::{File, OpenOptions};
use std::net::{TcpStream};
use std::io;
use std::io::{Write, Read, SeekFrom, Seek};
use inotify::{Inotify, WatchMask, WatchDescriptor };
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;

use super::er::Er;
use super::trace;
use super::config::{Config, TopicConfig};
use super::tcp::RecordType;
use super::consumer::ConsumerClient;

pub struct Topic {
    index : u64,
    data_file : File,  /* there are the 'current' files only */
    index_file : File, 
    data_file_name : String,  /* there are the 'current' files only */
    index_file_name : String, 
    current_producer : Option<u32>,
    pub last_data_offset : u64,
    pub last_index_offset : u64,
    is_producer : bool,
    config : TopicConfig,
    followers : HashSet<u32>,
}
impl Topic {

    pub fn open (config : TopicConfig, is_producer : bool) -> Result<Topic, Er>  {

        let f_data_name = Topic::latest_file_name('d', &config)?;
        let f_index_name = Topic::latest_file_name('i', &config)?;

        let mut f_data = Self::file_opener(is_producer).open(&f_data_name)
            .map_err(|e| Er::CantOpenFile(e))?;

        let mut f_index = Self::file_opener(is_producer).open(&f_index_name)
            .map_err(|e| Er::CantOpenFile(e))?;

        let last_index = f_index.seek(SeekFrom::End(0)).unwrap(); 
        let last_data = f_data.seek(SeekFrom::End(0)).unwrap(); 
        let idx = last_index / 8;

        Ok(Topic {
            index : idx,
            data_file : f_data,
            index_file : f_index,
            data_file_name : f_data_name,
            index_file_name : f_index_name,
            current_producer : None,
            last_data_offset : last_data,
            last_index_offset : last_index,
            is_producer : is_producer,
            config : config,
            followers : HashSet::new(),
        })
    }

    fn latest_file_name(prefix : char, config : &TopicConfig) -> Result<String, Er> {

        let topic_folder = format!("{}/{}", &config.folder, &config.topic_name);

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

            if first_char == prefix {
                let file_number = u64::from_str_radix(&f_name[1..], 16)
                    .map_err(|e| Er::BadOffset(String::from(f_name), e))?;

                if file_number > latest_file_number { 
                    latest_file_number = file_number
                }
            }
        }

        Ok(format!("{}/{}/{}{:016x}", config.folder, config.topic_name, prefix, latest_file_number))
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

    pub fn switch_file(&mut self, file_name : &str) -> Result<(), Er> {
        match file_name.chars().nth(0) {
            Some('i') => {
                self.index_file_name = String::from(file_name);
                self.index_file = Self::file_opener(false).open(file_name) //false here as this is only called by consumers
                    .map_err(|e| Er::CantOpenFile(e))?;
                Ok(())
            },
            Some('d') => {
                self.data_file_name = String::from(file_name);
                self.data_file = Self::file_opener(false).open(file_name) //false here as this is only called by consumers
                    .map_err(|e| Er::CantOpenFile(e))?;
                Ok(())
            },
            _ => {
                Err(Er::BadFileName)
            },
        }
    }

    pub fn write(&mut self, slice : &[u8]) -> Result<usize, Er> {
        let written = self.data_file.write(slice)
            .map_err(|e| Er::CantWriteFile(e))?;
        Ok(written)
    }

    pub fn end_rec(&mut self) -> Result<u64, Er> {
        let idx = self.index;
        self.index += 1;

        let file_position = self.data_file.seek(SeekFrom::Current(0))
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

    pub fn send_followers (&mut self, client_list: &mut HashMap<u32, ConsumerClient>, feed_type: RecordType) -> Result<Option<usize>, Er> {

        let client_count = self.followers.len();
        let mut client_no = 0;

        let mut size_to_send: Option<usize> = None;

        let file = match feed_type {
            RecordType::IndexFeed => self.index_file.as_raw_fd(),
            RecordType::DataFeed => self.data_file.as_raw_fd(),
            _ => return Err(Er::BadFileName),
        };

        for client_id in self.followers.iter() {
            if let Some(client) = client_list.get_mut(&client_id) {
                client_no += 1;

                let is_last : bool = (client_count == client_no);

                let socket = client.tcp.as_raw_fd();

                let n = Self::linux_send_file(socket, file, size_to_send, is_last)?;

                if n < 0 {
                    return Err(Er::CantSendFile(io::Error::last_os_error()))
                } else { 
                    if size_to_send.is_none() {
                        size_to_send = Some(n as usize);
                    }
                }
            }
        }
        Ok(size_to_send)
    }

    fn linux_send_file (socket:RawFd, file:RawFd, send_size:Option<usize>, is_last:bool) -> Result<isize, Er> {
        trace!("calling sendfile"); 
        let mut zero_offset = 0;
        let null: *mut i64 = ptr::null_mut();

        // zero offset and null differ as null will update the file descriptor with new position

        let n = match send_size {
            Some(n) =>  if is_last { 
                            unsafe { libc::sendfile(socket, file, null, n) }
                        } else {
                            unsafe { libc::sendfile(socket, file, &mut zero_offset, n) } 
                        },

            None    =>  if is_last { 
                            unsafe { libc::sendfile(socket, file, null, 0x7ffff000 ) }
                        } else {
                            unsafe { libc::sendfile(socket, file, &mut zero_offset, 0x7ffff000 ) }
                        },
        };
        trace!("sendfile returned {} ", n); 
        Ok(n)
    }

    pub fn follow(&mut self, client_id : u32) -> Result<(u64, u64), Er> {
        self.followers.insert(client_id);
        Ok((self.last_index_offset, self.last_data_offset))
    }

    #[cfg(test)] 
    pub fn get_data_file_name(&self) -> String { self.data_file_name.clone() }

    #[cfg(test)] 
    pub fn get_index_file_name(&self) -> String { self.index_file_name.clone() }

    #[cfg(test)] 
    pub fn get_config(&self) -> TopicConfig { 

        self.config.clone() }

}

pub struct TopicList {
    topic_names : HashMap<String, u32>,
    topics : HashMap<u32, Topic>,
    pub watchers : HashMap<WatchDescriptor, u32>,
    pub notify : Inotify,
}
impl TopicList {

    pub fn init (is_producer : bool) -> Result<TopicList, Er> {

        let topic_names : HashMap<String, u32> = HashMap::new();
        let topics : HashMap<u32, Topic> = HashMap::new();
        let watchers : HashMap<WatchDescriptor, u32> = HashMap::new();
        let notify = Inotify::init().expect("Inotify initialization failed - does this linux kernel support inotify?");
        let mut config = Config::new();

        let mut topic_list = TopicList {
            topic_names,
            topics,
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

}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::buff::Buff;
    use super::super::tcp::{BufferState, RecordType};
    use super::super::test_support::TestEnvironment;
    use super::super::test_support::TestTopic;

    use std::net::{TcpListener};
    use std::thread::sleep;
    use std::time::Duration;
    use std::fs;

    #[test]
    fn send_file() {
        let mut file = std::fs::File::create("/tmp/data.txt").expect("create failed");
        file.write_all("Hello World".as_bytes()).expect("write failed");

        let mut file_read = std::fs::File::open("/tmp/data.txt").expect("create failed");

        let addr = "127.0.0.1:34293"; 

        let listener = TcpListener::bind(&addr).unwrap();
        let mut client_stream = TcpStream::connect(&addr).unwrap();
        client_stream.set_read_timeout(Some(Duration::new(1, 0)));

        let mut server_stream = listener.incoming().next().unwrap().unwrap();

        let f = file_read.as_raw_fd();
        let s = server_stream.as_raw_fd();
        trace!("sending file");

        Topic::linux_send_file (s, f, None, false);
        let mut buffer1 = String::new();
        client_stream.read_to_string(&mut buffer1);
        assert_eq!(buffer1, "Hello World", "testing result 1 (full string no update)");

        Topic::linux_send_file (s, f, Some(5), true);
        let mut buffer3 = String::new();
        client_stream.read_to_string(&mut buffer3);
        assert_eq!(buffer3, "Hello", "testing result 3 (5 bytes with update");

        Topic::linux_send_file (s, f, None, true);
        let mut buffer4 = String::new();
        client_stream.read_to_string(&mut buffer4);
        assert_eq!(buffer4, " World", "testing result 4 (rest of file from pos of last update");


    }

    #[test]
    fn send_follower() {
        let env = TestEnvironment::new("send_follower");
        let mut t = Topic::test_new(&env, 2, "test2", false);
        let mut t_producer = t.test_open(true);

        t_producer.write(b"hello");


        // set up tcp client 
        let addr = "127.0.0.1:34292"; 

        let listener = TcpListener::bind(&addr).unwrap();
        let mut client_stream = TcpStream::connect(&addr).unwrap();
        client_stream.set_read_timeout(Some(Duration::new(1, 0)));

        let mut server_stream = listener.incoming().next().unwrap().unwrap();

        let buff = Buff::new();

        let c = ConsumerClient::new(1, server_stream);

        let mut client_list : HashMap<u32, ConsumerClient> = HashMap::new();
        client_list.insert(1, c);

        // run test
        t.follow(1);
        let r = t.send_followers(&mut client_list, RecordType::DataFeed); 

        match r {
            Err(e) => assert!(false, "check sendfile 1 worked {}", e),
            Ok(Some(n)) => assert_eq!(n , 5,  "checking at lest 5 bytes written"),
            Ok(None) => assert!(false, "nothing written to stream"),
        }

        trace!("start read as client");
        let mut buffer = String::new();
        client_stream.read_to_string(&mut buffer);
        assert_eq!(String::from("hello"), buffer, "strings don't match");

        t_producer.write(b"world");

        let r2 = t.send_followers(&mut client_list, RecordType::DataFeed); 

        match r2 {
            Err(e) => assert!(false, "check sendfile 1 worked {}", e),
            Ok(Some(n)) => assert_eq!(n , 5,  "checking at lest 5 bytes written"),
            Ok(None) => assert!(false, "nothing written to stream"),
        }

        let mut buffer2 = String::new();
        client_stream.read_to_string(&mut buffer2);
        assert_eq!(String::from("world"), buffer2, "strings don't match");

    }
/*
    #[test]
    fn zero_copy() {
        let topic_config = TopicConfig {
            topic_id : 1,
            topic_name : String::from("test"),
            folder : String::from("/tmp"),
            replication : 0,
            file_mask : 0,
        };

        let mut t = Topic::open(topic_config, false).expect("trying to create topic");

        let addr = "127.0.0.1:34292"; 

        let listener = TcpListener::bind(&addr).unwrap();
        let mut client_stream = TcpStream::connect(&addr).unwrap();
        client_stream.set_read_timeout(Some(Duration::new(1, 0)));

        let mut server_stream = listener.incoming().next().unwrap().unwrap();

        let r = t.send_data(&server_stream, 5, RecordType::DataFeed);

        match r {
            Err(e) => assert!(false, "check sendfile 1 worked {}", e),
            Ok(n) => assert_eq!(n, 5),
        }

        trace!("now read as client");
        let mut buffer = String::new();
        client_stream.read_to_string(&mut buffer);
        assert_eq!(String::from("hello"), buffer, "strings don't match");

        let r2 = t.send_data(&server_stream, 5);

        match r2 {
            Err(e) => assert!(false, "check sendfile 2 worked {}", e),
            Ok(n) => assert_eq!(n, 5),
        }

        trace!("now read as client");
        buffer = String::new();
        client_stream.read_to_string(&mut buffer);
        assert_eq!(String::from("hello"), buffer, "strings don't match");
    }
    */

    #[test]
    fn latest_file() {
        fs::create_dir("/tmp/testx");
        File::create("/tmp/testx/d0000000000000000");
        File::create("/tmp/testx/i0000000000000000");
        File::create("/tmp/testx/d000000000000001c");
        File::create("/tmp/testx/i000000000000001c");
        File::create("/tmp/testx/d00000000000000a3");
        File::create("/tmp/testx/i00000000000000a3");

        let config = TopicConfig {
            topic_id : 1,
            topic_name : String::from("testx"),
            folder : String::from("/tmp"),
            replication : 0, 
            file_mask : 8,
        };

        let latest_data_name = Topic::latest_file_name('d', &config);
        assert!(latest_data_name.is_ok(), "trying to get latest index file without error"); 
        assert_eq!(latest_data_name.unwrap(), "/tmp/testx/d00000000000000a3", "trying to get latest data file");

        let latest_index_name = Topic::latest_file_name('i', &config);
        assert!(latest_index_name.is_ok(), "trying to get latest index file without error");
        assert_eq!(latest_index_name.unwrap(), "/tmp/testx/i00000000000000a3", "trying to get latest index file");
    }

    #[test]
    fn test_topicwrite() {
        let env = TestEnvironment::new("topicwrite_env");
        let mut t = Topic::test_new(&env, 1, "test", true);

        let idx = t.index;
        let written : usize = t.write(&b"hello"[..]).expect("trying to write to file");
        assert_eq!(written, 5, "5 bytes should be written to file");
        t.end_rec();
        assert_eq!(t.index, idx + 1, "checking index is incremented by one");
    }

    #[test]
    fn test_readfirstrecord() {
        let env = TestEnvironment::new("readfirstrecord");
        let mut t = Topic::test_new(&env, 1, "test1", false);

        let content = b"First Record";
        t.create_entries(content, 2, 2);

        let mut buf : [u8; 8] = [0;8];

        match t.read_index_into(&mut buf, 0) {
            Err(e) => assert!(false, "error reading index {}", e),
            Ok(n) => assert_eq!(n, 8, "check 8 index bytes read"),
        }

        let idx = u64::from_le_bytes(buf);
        trace!("read index {}", idx);

        assert_ne!(idx, 0, "check index value is not zero");

        let mut databuf : [u8;1024] = [0;1024];
        match t.read_data_into(&mut databuf[..idx as usize], 0) {
            Err(e) => assert!(false, "error reading data {}", e),
            Ok(n) => assert_eq!(n, idx as usize, "check {} data bytes read", n),
        }

        assert_eq!(databuf[0], content[0], "check first char");
        assert_eq!(databuf[11], content[11], "check last char");
    }

    #[test]
    #[ignore]
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

