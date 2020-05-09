use std::io::{Result, Write};
use std::net::TcpStream;

pub struct Client {
    io : TcpStream,
    seq : u8,
}
impl Client {
    pub fn new (topic : String, url : String, auth : String) -> Result<Client> {

        let mut stream = TcpStream::connect(url)?;
        let message = format!("{};{}",topic, auth);
        
        let size = message.len() as u32;
        let mess_type : u8 = 1; // 1 = auth
        let topic_id : u16 = 0; // 1 = auth
        let seq : u8 = 1;

        stream.write(&size.to_le_bytes())?;
        stream.write(&[seq])?;
        stream.write(&[mess_type])?;
        stream.write(&topic_id.to_le_bytes())?;
        stream.write(message.as_bytes())?;

        Ok (Client { io : stream, seq : seq + 1 })
    }

    pub fn send(&mut self, content : String) -> Result<()> {

        let len : u32 = content.len() as u32;
        let mess_type : u8 = 1; // 1 = auth
        let topic_id : u16 = 1; // 1 = auth

        self.io.write(&len.to_le_bytes())?;
        self.io.write(&[self.seq])?;
        self.io.write(&[mess_type])?;
        self.io.write(&topic_id.to_le_bytes())?;

        self.io.write(content.as_bytes())?;
        println!("sent {}", content);
        self.seq = self.seq % 255 + 1;

        Ok(())
    }
}
