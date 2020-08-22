#[cfg(test)]
mod tests {
    use redfoam::client::Client;
    use redfoam::tcp;
    use redfoam::er::Er;
    use redfoam::trace;
    use std::time::Duration;
    use std::thread;

    fn setup() {

        thread::spawn(move || {
            tcp::run_server(String::from("127.0.0.1:9090"));
        });

        thread::spawn(move || {
            tcp::run_consumer_server(String::from("127.0.0.1:9091"));
        });
        thread::sleep(Duration::new(3,0));
    }

#[ignore]
#[test]
    fn writesomething () {

        let mut c = Client::new(String::from("test"), String::from("127.0.0.1:9090"), String::from("ANON")).unwrap();
        c.send(String::from("hello world!")).expect("sending hello world failed");
        c.send(String::from("another message")).expect("sending another message failed");
        thread::sleep(Duration::new(1,0));
    }

#[test]
    fn readsomething () -> Result<(), Er> {
        setup();
        let mut producer = Client::new(String::from("test"), String::from("127.0.0.1:9090"), String::from("ANON")).unwrap();
        let mut consumer = Client::new(String::from("test"), String::from("127.0.0.1:9091"), String::from("ANON")).unwrap();

        println!("follow topic****");
        consumer.follow_topic(1);
        producer.send(String::from("alphabet soup")).expect("sending alphabetsoup failed");
        thread::sleep(Duration::new(1,0));

        println!("read something .... next()");
        match consumer.next()? {
            Some(tcp::RecordType::DataFeed) => {
                trace!("got data {}", String::from_utf8_lossy(consumer.data())); 
                println!("got data {}", String::from_utf8_lossy(consumer.data())); 
                assert_eq!("alphabet soup", std::str::from_utf8(consumer.data()).unwrap());
            },
            Some(tcp::RecordType::IndexFeed) => {
                assert!(false, "got indexfeed");
            },
            Some(x) => {
                assert!(false, "got something unexpected! type {}, buffer {:?}", x as u64, consumer.data());
            }
            None => {
                assert!(false, "didn't get anything");
            }
        }

        consumer.reset();

        match consumer.next()? {
            Some(tcp::RecordType::DataFeed) => {
                assert_eq!("alphabet soup", std::str::from_utf8(consumer.data()).unwrap());
            },
            Some(tcp::RecordType::IndexFeed) => {
                assert!(false, "got indexfeed {:?}", consumer.data());
            },
            Some(x) => {
                assert!(false, "got something unexpected! type {}, buffer {:?}", x as u64, consumer.data());
            }
            None => {
                assert!(false, "didn't get anything");
            }
        }
        Ok(())
    }
}
