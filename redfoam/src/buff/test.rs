use super::*;

#[test]
fn test_buffbasic() {
    let mut b = Buff::new();


    let result1 = b.read_u8();
    assert!(result1.is_none(), "should be none as pointer is still at 0");

    b.buffer[0] = 0x44u8;
    b.buff_pos+=1;

    let result2 = b.read_u8();
    assert!(result2.is_some(), "should have read value for u8 successfully");
    assert_eq!(Some(0x44u8), result2, "u8 value should match first element of array");

    b.rec_size = Some(14);
    assert_eq!(b.has_data(), false, "has_data() should return false as buffer pos was set to only 1 char");

    assert_eq!(b.is_end_of_record(), false, "only one char read, but record size is 14 char, so not end of record");
    
    let x_u16: u16 = 309;
    let a_u16 = x_u16.to_le_bytes();

    b.buffer[1] = a_u16[0];
    b.buffer[2] = a_u16[1];
    b.buff_pos+=5;

    let result3 = b.read_u16();
    assert!(result3.is_some(), "should have read value for u16 successfully");
    assert_eq!(Some(309), result3, "u16 value should be 309");

}

#[test]
fn test_readbuff() {
    let mut bstr : &[u8] = b"\x12\x00\x00\x00Eat My Shorts!"; 
    let mut b = Buff::new();

    match b.read_data(&mut bstr) {
        Ok(sz) => assert_eq!(sz, 18),
        Err(e) => assert!(false, "read data failure with {}", e),
    }

    let result1 = b.read_u32();
    assert!(result1.is_some(), "should be able to read u32");
    assert_eq!(result1, Some(18), "should be size of data (14 or x12)"); 

    b.rec_size=result1;

    let message : &[u8] = b"Eat My Shorts!"; 

    assert_eq!(b.data(), message, "should be full message 'Eat My Shorts!' without size bytes"); 
    assert_eq!(b.is_end_of_record(), true, "should know it is at end of record");
    b.reset();


}

#[test]
fn test_fullbuff() {
    let mut bstr : &[u8] = b"\x80\x06\x00\x00012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\
    0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"; 

    let mut b = Buff::new();

    match b.read_data(&mut bstr) {
        Ok(sz) => assert_eq!(sz, 1024),
        Err(e) => assert!(false, "read data failure with {}", e),
    }

    let result1 = b.read_u32();
    assert!(result1.is_some(), "should be able to read u32");
    assert_eq!(result1, Some(1664), "should be size of data (1664 or x0680)"); 

    b.rec_size=result1;
    assert_eq!(b.data().len(), 1020 as usize, "should be the first 1020 bytes of message"); 
    assert_eq!(b.is_end_of_record(), false, "haven't read full record"); 
    assert_eq!(bstr.len(), 1660 - 1020, "checking input string has 640 left");
    
    b.reset();

    match b.read_data(&mut bstr) {
        Ok(sz) => assert_eq!(sz, 1660 - 1020, "checking read in another 640 bytes"),
        Err(e) => assert!(false, "read data failure with {}", e),
    }
    b.reset();
    assert_eq!(b.seq, 1, "sequence should still be 1");
}

#[test]
fn test_multirec() {
    let mut bstr : &[u8] = b"\x12\x00\x00\x00Eat My Shorts!\x28\x00\x00\x00I have bumble bees in my back garden"; 
    let mut b = Buff::new();

    match b.read_data(&mut bstr) {
        Ok(sz) => assert_eq!(sz, 58),
        Err(e) => assert!(false, "read data failure with {}", e),
    }

    let result1 = b.read_u32();
    assert!(result1.is_some(), "should be able to read u32");
    assert_eq!(result1, Some(18), "should be size of data (18 or x12)"); 

    b.rec_size=result1;

    let message : &[u8] = b"Eat My Shorts!"; 

    assert_eq!(b.data(), message, "should be full message 'Eat My Shorts!' without size bytes"); 
    b.reset();
    assert_eq!(b.seq, 1, "sequence should be 2");

    let result2 = b.read_u32();
    assert!(result2.is_some(), "should be able to read u32");
    assert_eq!(result2, Some(40), "should be size of data (40 or x28)"); 

    b.rec_size=result2;

    let message2 : &[u8] = b"I have bumble bees in my back garden"; 
    assert_eq!(b.data(), message2, "should be full message 'I have bumble bees in my back garden' without size bytes"); 
    b.reset();

    assert_eq!(b.seq, 2, "sequence should be 2");
    
}
