
#[macro_use]
macro_rules! make_server {
    ($typename: ident, $handler : ty) => {
        pub struct $typename {
            rx :  mpsc::Receiver<TcpStream>,
            client_list : Vec<$handler>,
            topic_list : TopicList,
        }
        impl $typename {
            pub fn new (rx :  mpsc::Receiver<TcpStream>) -> $typename {
                $typename { 
                    rx : rx,
                    client_list : Vec::new(),
                    topic_list : TopicList::new(),
                }
            }

            pub fn run (&mut self) { 
                loop {

                    match self.rx.try_recv() {
                        Ok(instream) => {
                            println!("creating new client");
                            let c = <$handler>::new(instream);
                            self.client_list.push(c);
                        },
                        Err(mpsc::TryRecvError::Empty) => { }, // no new stream - do nothing
                        Err(_e) => { panic!("  OOOPSS");  }
                    }

                    self.client_list.retain(|c| match c.state() {
                        BufferState::Closed => false, _ => true 
                    });

                    for c in self.client_list.iter_mut() {
                        c.process(&mut self.topic_list);
                    }


                    thread::sleep(Duration::from_millis(100))
                }
            }
        }
    }
}

pub mod client;
pub mod producer;
pub mod tcp;
pub mod topic;
pub mod buff;
