
pub mod Client {
    use std::io::{Result, Write};
    use std::net::TcpStream;

    pub struct Client {
        topic_id : u32,
        io : TcpStream,
    }

    impl Client {
        pub fn new (topic : String, url : String) -> Result<Client> {

            let stream = TcpStream::connect("127.0.0.1:9090")?;

            Ok ( Client { topic_id : 1, io : stream, } )
        }

        pub fn Send(&mut self, content : String) -> Result<()> {
            self.io.write(content.as_bytes())?;
            Ok(())
        }
    }
}
