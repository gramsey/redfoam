#[cfg(test)]
mod tests {
    use redfoam::client::*;
#[test]
    fn writesomething () {
        
        let mut c = Client::new(String::from("mytopic"), String::from("127.0.0.1:9090")).unwrap();
        c.SendRaw(&0xAu32.to_le_bytes());
        c.Send(String::from("123456790"));
        c.Send(String::from("Hello there"));
    }
#[test]
    fn readsomething () {
        panic!("not written yet");
    }
}
