use std::io::{Read, ErrorKind};
use std::convert::TryInto;

use super::er::Er;

pub const BUFF_SIZE : usize = 1024;


pub struct Buff {
    pub  rec_size : u32,
    pub buffer : [u8 ; BUFF_SIZE],
    buff_pos : u32,
    rec_pos : u32,
    rec_upto : u32,
}
impl Buff {
    pub fn new() -> Buff {
        Buff {
            rec_size : 0, // size of record excluding 4 byte size
            buffer : [0; BUFF_SIZE], // actual buffer containing data
            buff_pos : 0, // position of end of buffer (read from tcp) 
            rec_pos : 0,  // position of last byte processed in buffer
            rec_upto : 0, // no of bytes processed so far in record
        }
    }

    pub fn read_data(&mut self, stream : &mut impl Read) -> Result<usize, Er>  {
        match stream.read(&mut self.buffer[self.buff_pos as usize..BUFF_SIZE]) {
            Ok(size) => {
                self.buff_pos += size as u32;
                println!("read_data buff_pos {}, rec_size {}, rec_pos {}, rec_upto {}", self.buff_pos, self.rec_size, self.rec_pos, self.rec_upto);
                Ok(size)
            },
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                Ok(0)
            },
            Err(e) => {
                Err(Er::ClientTcpRead(e))
            },
        }
    }

    pub fn read_u8(&mut self) -> u8 {
        let start = self.rec_pos as usize;
        let result = self.buffer[start];
        self.rec_pos += 1;
        result
    }
    pub fn read_u16(&mut self) -> u16 {
        let start = self.rec_pos as usize;
        let end = start + 2;
        let result = u16::from_le_bytes(self.buffer[start..end].try_into().expect("slice with incorrect length"));
        self.rec_pos += 2;
        result
    }
    pub fn read_u32(&mut self) -> u32 {
        let start = self.rec_pos as usize;
        let end = start + 4;
        let result = u32::from_le_bytes(self.buffer[start..end].try_into().expect("slice with incorrect length"));
        self.rec_pos += 4;
        result
    }
    pub fn read_u64(&mut self) -> u64  {
        let start = self.rec_pos as usize;
        let end = start + 8;
        let result = u64::from_le_bytes(self.buffer[start..end].try_into().expect("slice with incorrect length"));
        self.rec_pos += 8;
        result
    }

    pub fn has_data(&self) -> bool {
        self.read_to() as u32 - self.rec_pos > 0
    }

    pub fn has_bytes_to_read(&self, n : u32) -> bool {
        println!("buff_pos {}, rec_pos {}", self.buff_pos, self.rec_pos);
        self.buff_pos >= self.rec_pos + n 
    }

    pub fn is_end_of_record(&self) -> bool {
        let size = self.read_to() as u32 - self.rec_pos;
        self.rec_upto + size == self.rec_size 
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[self.rec_pos as usize .. self.read_to()]
    }

    fn read_to(&self) -> usize {
        let buff_toread = self.buff_pos - self.rec_pos;
        let rec_toread = self.rec_size - self.rec_upto;

        if buff_toread <=  rec_toread {
            self.buff_pos as usize
        } else {
            //read just up to end of record
            (self.rec_pos + rec_toread) as usize
        }
    }

    pub fn reset(&mut self) {
        println!("reset");
        // update for next read
        let size = self.read_to() as u32 - self.rec_pos;
        self.rec_pos += size;
        self.rec_upto += size;

        // if all buffer is read reset 
        if self.buff_pos == self.rec_pos {
            self.buff_pos = 0;
            self.rec_pos = 0;
        } else if self.buff_pos < self.rec_pos {
            panic!("record beyond end of buffer");
        }

        // if record is completely written reset
        if self.is_end_of_record() {
            println!("is end of record");
            self.rec_upto = 0;
            self.rec_size = 0;
        } else if self.rec_upto > self.rec_size {
            panic!("read past end of record? shouldn't happen...");
        }
    }
}
