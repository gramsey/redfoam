
pub struct ConsumerServer {
    rx :  mpsc::Receiver<TcpStream>,
}
impl ConsumerServer {
    pub fn new (rx :  mpsc::Receiver<TcpStream>) -> ConsumerServer {
        ConsumerServer { rx }
    }

    pub fn run (&self) { 
        let mut client_list : Vec<ClientBuff> = Vec::new();
        let mut topic_list : HashMap<String, Topic> = HashMap::new();
        topic_list.insert(String::from("test"), Topic::new(String::from("test")));
        let mut next_producerid : u32 = 0;

        loop {
            // add new client if sent
            let message = self.rx.try_recv();

            next_producerid += 1;

            match message {
                Err(mpsc::TryRecvError::Empty) => {
                    //no new stream do nothing
                },

                Ok(instream) => {
                    println!("creating new client");
                    let c = ClientBuff::new(instream, next_producerid);
                    client_list.push(c);
                },
                
                Err(_e) => {
                    println!("  OOOPSS");  //todo: proper error handling
                }
            }

            // process existing clients - go backwards as list will change length
            for i in (0..client_list.len()).rev() {
                let c = &mut client_list[i];

                match c.state {
                    BufferState::Pending => {
                        c.process_auth();
                    },

                    BufferState::Active => {
                        c.process_data(&mut topic_list);
                    },

                    BufferState::Closed => {
                        client_list.remove(i);
                    },

                }
            }
            thread::sleep(Duration::from_millis(100))
        }
    }
}

