#[cfg(test)]
mod tests {
    use redfoam::client::Client;
    use redfoam::tcp;
    use std::process::Command;
    use std::time::Duration;
    use std::thread;

    fn setup() {
        thread::spawn(move || {
            tcp::run_server(String::from("127.0.0.1:9090"));
        });
        thread::sleep(Duration::new(1,0));
    }

#[test]
    fn writesomething () {
        setup();

        let mut c = Client::new(String::from("test"), String::from("127.0.0.1:9090"), String::from("ANON")).unwrap();
        c.send(String::from("hello world!")).expect("sending hello world failed");
        thread::sleep(Duration::new(1,0));
    }


#[test]
#[ignore]
    fn readsomething () {
        setup();
        panic!("not written yet");
    }
}
