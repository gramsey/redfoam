use std::io::{Result, Write};
use std::net::TcpStream;

pub struct Client {
    io : TcpStream,
}
impl Client {
    pub fn new (topic : String, url : String, auth : String) -> Result<Client> {

        let mut stream = TcpStream::connect(url)?;
        
        let size : u32 = (4 + topic.len() + 1 + auth.len()) as u32;

        stream.write(&size.to_le_bytes())?;
        stream.write(topic.as_bytes())?;
        stream.write(";".as_bytes())?;
        stream.write(auth.as_bytes())?;

        Ok (Client { io : stream })
    }

    pub fn send(&mut self, content : String) -> Result<()> {

        let len : u32 = (4 + content.len()) as u32;
        self.io.write(&len.to_le_bytes())?;

        self.io.write(content.as_bytes())?;
        Ok(())
    }
}
