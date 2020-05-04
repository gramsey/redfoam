#[cfg(test)]
mod tests {
    use redfoam::client::Client;
    use std::process::Command;
    use std::time::Duration;

    static mut INIT_DONE : bool = false;
#[test]
    fn writesomething () {
        setup();

        let mut c = Client::new(String::from("test"), String::from("127.0.0.1:9090"), String::from("ANON")).unwrap();
        c.send(String::from("hello world!"));
    }
#[test]
#[ignore]
    fn readsomething () {
        setup();
        panic!("not written yet");
    }

    fn setup() {
        // unsafe {
        //     if !INIT_DONE {
        //         Command::new("sh").arg("-c").arg("cargo run");

        //         let second = Duration::new(1, 0);
        //         std::thread::sleep(second);

        //         INIT_DONE = true;
        //     }
        // }
    }
}
