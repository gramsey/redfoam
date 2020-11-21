use std::io::{Read, ErrorKind};
use std::convert::TryInto;
use std::mem;
use std::fmt;

use super::er::{Er, LogError};

use super::trace;

pub const BUFF_SIZE:usize = 1024;

macro_rules! make_read_fn {
    ($fn_name: ident, $fn_type:ty) => {

        pub fn $fn_name (&mut self) -> Option<$fn_type> {
            let size:usize = mem::size_of::<$fn_type>();
            if self.has_n_bytes(size) {
                let start = self.rec_pos as usize;
                let end = start + size;
                let result = <$fn_type>::from_le_bytes(self.buffer[start..end].try_into()
                    .map_err(|e| Er::ParseError(format!("slice with {} incorrect length for {}, Error: {}", size, "$fn_type", e)))
                    .handle_err("Error reading number from bits in buffer")); //this error should never ever happen
                self.rec_pos += size as u32;
                self.rec_upto += size as u32;
                Some(result)
            } else {
                None
            }
        }
    }
}

pub struct Buff {
    pub  rec_size:Option<u32>,
    pub buffer:[u8 ; BUFF_SIZE],
    buff_pos:u32,
    rec_pos:u32,
    rec_upto:u32,
    pub seq:u8,
    seq_checked:bool,
}
impl Buff {
    pub fn new() -> Buff {
        Buff {
            rec_size : None, // size of record excluding 4 byte size and any other fixed size headers
            buffer : [0; BUFF_SIZE], // actual buffer containing data
            buff_pos : 0, // position of end of buffer (read from tcp) 
            rec_pos : 0,  // position of last byte processed in buffer
            rec_upto : 0, // no of bytes processed so far in record
            seq : 0,
            seq_checked : false,
        }
    }

    make_read_fn!(read_u16, u16);
    make_read_fn!(read_u32, u32);
    make_read_fn!(read_u64, u64);
    pub fn read_u8 (&mut self) -> Option<u8> {
        if self.has_n_bytes(1) {
            let result = self.buffer[self.rec_pos as usize];
            self.rec_pos += 1;
            self.rec_upto += 1;
            Some(result)
        } else {
            None
        }
    }

    pub fn read_data(&mut self, stream: &mut impl Read) -> Result<usize, Er>  {
        match stream.read(&mut self.buffer[self.buff_pos as usize..BUFF_SIZE]) {
            Ok(size) => {
                self.buff_pos += size as u32;
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

    pub fn check_seq(&mut self) -> Result<(),Er> {
        if !self.seq_checked {

            if let Some(s) = self.read_u8() {
                if s == self.seq { 
                    self.seq_checked = true;
                    Ok(())
                } else {
                    Err(Er::InvalidSequence) 
                }
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    pub fn has_data(&self) -> bool {
        self.read_to() as u32 - self.rec_pos > 0
    }

    fn has_n_bytes(&self, n:usize) -> bool {
        self.buff_pos >= self.rec_pos + n as u32
    }

    pub fn is_end_of_record(&self) -> bool {
        if let Some(rec_size) = self.rec_size {
            let size = self.read_to() as u32 - self.rec_pos;
            self.rec_upto + size == rec_size 
        } else {
            false
        }
    }

    pub fn data(&self) -> &[u8] {
        trace!("[data()] {:?}", self);
        trace!("[read_to()] {}", self.read_to());
        &self.buffer[self.rec_pos as usize .. self.read_to()]
    }

    fn read_to(&self) -> usize {
        if let Some(rec_size) = self.rec_size {
            let buff_toread = self.buff_pos - self.rec_pos;
            let rec_toread = rec_size - self.rec_upto;

            if buff_toread <=  rec_toread {
                self.buff_pos as usize
            } else {
                //read just up to end of record
                (self.rec_pos + rec_toread) as usize
            }
        } else {
            panic!("trying to read when there is no size set : this should never happen!!!");
        }
    }

    pub fn reset(&mut self) {
        trace!("reset before{:?}", self);
        // update for next read
        let size = self.read_to() as u32 - self.rec_pos;
        self.rec_pos += size;
        self.rec_upto += size;

        // if all buffer is read reset to start 
        if self.buff_pos == self.rec_pos {
            self.buff_pos = 0;
            self.rec_pos = 0;
        } else if self.buff_pos < self.rec_pos {
            panic!("record beyond end of buffer");
        }

        // if record is completely written reset
        if self.is_end_of_record() {
            self.rec_upto = 0;
            self.rec_size = None;
            self.seq = self.seq.wrapping_add(1); //overflows at 255 + 1 back to zero
            self.seq_checked = false;
        }
        trace!("reset after {:?}", self);
    }
}
impl fmt::Debug for Buff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buff")
            .field("rec_pos", &self.rec_pos)
            .field("buff_pos", &self.buff_pos)
            .field("rec_upto", &self.rec_upto)
            .field("rec_size", &self.rec_size)
            .field("seq", &self.seq)
            .field("content", &String::from_utf8_lossy(&self.buffer[..self.buff_pos as usize]))
            .finish()
    }
}

#[cfg(test)]
mod test;
