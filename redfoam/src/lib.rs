

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
    use std::convert::TryInto;

    const BUFF_SIZE : usize = 1024; //todo remove and make configureable

    pub struct ProducerServer {
        rx :  mpsc::Receiver<TcpStream>,
    }


    struct ClientBuff {
        auth_ok : bool,
        topic : Option<String>,
        buffer : [u8 ; BUFF_SIZE],
        buff_pos : u32,
        rec_size : u32,
        rec_pos : u32,
        rec_todo : u32,
        tcp : TcpStream,
        header : String,
    }

    impl ClientBuff {
        fn new (stream : TcpStream) -> ClientBuff {
            ClientBuff {
                auth_ok : false, 
                topic : None, 
                buffer : [0; 1024],
                buff_pos : 0,
                rec_size : 0,
                rec_pos : 0, 
                rec_todo : 0, 
                tcp : stream,
                header : String::from(""),
            }

        }

        fn process(&mut self) {
            if self.auth_ok {
                self.process_data();
            } else {
                self.process_auth();
            }
        }

        fn process_data(&mut self) {

        }

        fn validate_token(&self) -> bool {
            true
        }

        fn process_auth(&mut self) {

            match self.tcp.read(&mut self.buffer) {
                Ok(size) => {
                    self.buff_pos += size as u32;

                    if self.rec_size == 0 && self.buff_pos >= 4 {
                        self.rec_size = u32::from_le_bytes(self.buffer[0..4].try_into().expect("slice with incorrect length"));
                    }

                    if self.rec_size <= self.buff_pos {
                        self.auth_ok = self.validate_token();
                        self.rec_pos = self.rec_size;

                        if !self.auth_ok {
                            self.tcp.write("BAD REQUEST".as_bytes()).unwrap();
                            self.tcp.shutdown(Shutdown::Both).expect("shutdown call failed");
                        } else {
                            self.tcp.write("OK :)".as_bytes()).unwrap();
                        }
                    } else {
                        println!("got {} bytes - need {}", self.buff_pos, self.rec_size);
                    }
                     
                },

                Err(e) => {
                    println!("  OOOPSS"); 
                }
            }
        }
    }


    impl ProducerServer {

        pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ProducerServer {
            ProducerServer { rx }
        }

        pub fn run (&self) { 
            loop {
                let message = self.rx.recv();

                match message {
                    Ok(instream) => {
                        let mut c = ClientBuff::new(instream);
                        c.process();
                    },

                    Err(e) => {
                        println!("  OOOPSS"); 
                    }
                }
            }
        }
    }

//            let mut file = OpenOptions::new().append(true).open("/tmp/foo.txt").unwrap();
                    //file.write(&buf[0..size]).unwrap();

}
