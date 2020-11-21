use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Write, Read, SeekFrom, Seek};
use inotify::{Inotify, WatchMask, WatchDescriptor };
use std::collections::{HashMap, HashSet};
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

                let is_last:bool = client_count == client_no;

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
            topic_list.add_topic(topic_cfg, is_producer)?;
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
mod test;
