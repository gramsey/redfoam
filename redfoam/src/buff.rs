use std::io::{Read, ErrorKind};
use std::convert::TryInto;

pub const BUFF_SIZE : usize = 1024;

pub struct Buff {
    pub buffer : [u8 ; BUFF_SIZE],
    buff_pos : u32,
    rec_size : u32,
    rec_pos : u32,
    rec_upto : u32,
}
impl Buff {
    pub fn new() -> Buff {
        Buff {
            buffer : [0; BUFF_SIZE],
            buff_pos : 0, // position of end of buffer (read from tcp) 
            rec_size : 0, // size of record excluding 4 byte size
            rec_pos : 0,  // position of last byte processed in buffer
            rec_upto : 0, // no of bytes processed so far in record
        }
    }

    pub fn read_data(&mut self, stream : &mut impl Read) -> (usize, usize, bool) {
        match stream.read(&mut self.buffer[self.buff_pos as usize..BUFF_SIZE]) {
            Ok(size) => {
                self.buff_pos += size as u32;
            },
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
            },
            Err(_e) => {
                panic!("Error trying to read into clientbuff"); 
            },
        }

        // set record size 
        if self.rec_size == 0 && self.buff_pos >= self.rec_pos + 4 {
            let start = self.rec_pos as usize;
            let end = self.rec_pos as usize + 4 ;
            self.rec_size = u32::from_le_bytes(self.buffer[start..end].try_into().expect("slice with incorrect length"));
            self.rec_pos += 4;
        }

        if self.rec_size > 0 {
        // get data slice
            let buff_toread = self.buff_pos - self.rec_pos;
            let rec_toread = self.rec_size - self.rec_upto;
            let rec_end : usize;

            if buff_toread <=  rec_toread {
                rec_end = self.buff_pos as usize
            } else {
                //read just up to end of record
                rec_end = (self.rec_pos + self.rec_size) as usize
            }

            let rec_start = self.rec_pos as usize;

            // update for next read
            self.rec_pos += (rec_end - rec_start) as u32;
            self.rec_upto += (rec_end - rec_start) as u32;

            // if all buffer is read reset 
            if self.buff_pos == self.rec_pos {
                self.buff_pos = 0;
                self.rec_pos = 0;
            } else if self.buff_pos < self.rec_pos {
                panic!("record beyond end of buffer");
            }

            let mut is_end_of_record = false;
            // if record is completely written reset
            if self.rec_upto == self.rec_size {
                self.rec_upto = 0;
                self.rec_size = 0;
                is_end_of_record = true;
            } else if self.rec_upto > self.rec_size {
                panic!("read past end of record? shouldn't happen...");
            }
            (rec_start, rec_end, is_end_of_record)
        } else {
            (0, 0, false)
        }
    }
}
