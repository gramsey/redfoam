use std::fs::{File, OpenOptions};
use std::io::{Write, SeekFrom, Seek};
use std::str;

pub struct Topic {
    index : u32,
    data_file : File,
    index_file : File,
    current_producer : Option<u32>,
}

impl Topic {
    // new handle, topic must already exist as a sym link
    pub fn new (name : String) -> Topic  {
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

    pub fn write(&mut self, slice : &[u8]) -> usize {
        println!("content :_{}_", str::from_utf8(slice).expect("failed to format"));
        let written = self.data_file.write(slice).unwrap();
        written
    }

    pub fn end_rec(&mut self) {
        let file_position = self.data_file.seek(SeekFrom::End(0)).unwrap();
        let file_position_bytes = (file_position as u64).to_le_bytes(); // get start instead of end position
        self.index_file.write( &file_position_bytes).unwrap();
        self.index += 1;
        self.current_producer = None;
    }
}
