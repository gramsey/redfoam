#[cfg(test)]
mod tests {
    use super::*;
    use redfoam::client::*;
#[test]
    fn writesomething () {
        
        let mut c = Client::new(String::from("mytopic"), String::from("127.0.0.1:9090")).unwrap();
        c.SendRaw(10.as_bytes)
        c.Send(String::from("hello world"));
    }
#[test]
    fn readsomething () {
        panic!("not written yet");
    }
}
