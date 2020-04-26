#[cfg(test)]
mod tests {
    use super::*;
    use redfoam::Client::*;
#[test]
    fn writesomething () {
        let mut c = Client::new(String::from("mytopic"), String::from("127.0.0.1:9090")).unwrap();
        c.Send(String::from("hello world"));
    }
#[test]
    fn readsomething () {
        panic!("not written yet");
    }
}
