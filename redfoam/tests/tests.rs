#[cfg(test)]
mod tests {
    use redfoam::client::*;
#[test]
    fn writesomething () {
        
        let mut c = Client::new(String::from("mytopic"), String::from("127.0.0.1:9090")).unwrap();
        c.SendRaw(&0x9u32.to_le_bytes());
        c.Send(String::from("12345")); // 9 = 5 for string, and 4 for u38 size
        c.Send(String::from("Hello there"));
    }
#[test]
    fn readsomething () {
        panic!("not written yet");
    }
}
