#[cfg(test)]
mod tests {
    use redfoam::client::Client;
#[test]
    fn writesomething () {

        let mut c = Client::new(String::from("test"), String::from("127.0.0.1:9090"), String::from("ANON")).unwrap();
        c.send(String::from("hello world!"));
    }
#[test]
    fn readsomething () {
        panic!("not written yet");
    }
}
