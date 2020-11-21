use super::*;

#[ignore]
#[test]
fn test_queue () {
    let message1 = b"This is the message";
    let message2 = b"Wow This is the second message";
    let message2a = &message2[..5];
    let message2b = &message2[5..];

    let mut q = Messages::new(100, 8);
    q.push_data(b"0123456789");
    q.push_data(message1);
    q.push_data(message2a);
    q.push_data(message2b);
    q.push_data(b"Some trailing junk...");

    let mut i = 100 + 10;
    q.push_index(i);
    i += message1.len() as u64;
    q.push_index(i);
    i += message2.len() as u64;
    q.push_index(i);

    let result1 = message1.to_vec();
    let result2 = message2.to_vec();
    
    assert_eq!(q.next(), None);
    assert_eq!(q.next(), Some(result1));
    assert_eq!(q.next(), Some(result2));
    assert_eq!(q.next(), None);
}
