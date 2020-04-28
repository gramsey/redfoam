

pub mod client {
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

pub mod producer {
    use std::fs::{File, OpenOptions};
    use std::sync::mpsc;
    use std::net::{TcpListener, TcpStream, Shutdown};
    use std::io::{Read, Write};

    pub struct ProducerServer {
        rx :  mpsc::Receiver<TcpStream>,
    }

    impl ProducerServer {

        pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ProducerServer {
            ProducerServer { rx }
        }

        pub fn get_client (&self) { 
            let message = self.rx.recv();
            match message {
                Ok(instream) => {
                    ProducerServer::handle_client(instream);
                },

                Err(e) => {
                    println!("  OOOPSS"); 
                }
            }
        }



        fn handle_client(mut stream: TcpStream) {
            let mut buf = [0; 1024];
            let mut file = OpenOptions::new().append(true).open("/tmp/foo.txt").unwrap();

            while match stream.read(&mut buf) {
                Ok(size) => {

                    stream.write(&buf[0..size]).unwrap();

                    file.write(&buf[0..size]).unwrap();

                    true
                },

                Err(_) => {
                    println!("Error with tcp stream to {}, (terminating connection)", stream.peer_addr().unwrap());
                    stream.shutdown(Shutdown::Both).unwrap();

                    false
                }
            } {}
        }
    }
}
