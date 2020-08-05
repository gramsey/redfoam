use std::str;

use super::buff::Buff;
use super::er::Er;

pub struct Auth {
    //client_id : u32,
}
impl Auth {
    pub fn new(buff : &Buff) -> Result<Option<Self>, Er> {

        if buff.is_end_of_record() {

            let content = str::from_utf8(buff.data()).unwrap();
            println!("content : {} ", content);

            let mut authpart = content.split(";");

            let topic_name = String::from(authpart.next().unwrap());
            println!("topic_name : {}", topic_name);


            let auth_token = authpart.next().unwrap();
            println!("auth_token : {}", auth_token);

            if auth_token == "ANON" {
                Ok(Some(Auth {}))
            } else {
                Err(Er::BadAuth)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth() {
        let mut bstr : &[u8] = b"\x0c\x00\x00\x00mytopic;ANON"; 
        let mut b = Buff::new();

        match b.read_data(&mut bstr) {
            Ok(sz) => assert_eq!(sz, 16),
            Err(e) => assert!(false, "read data failure with {}", e),
        }

        let result1 = b.read_u32();
        assert!(result1.is_some(), "should be able to read u32");
        assert_eq!(result1, Some(12), "should be size of data (14 or x0e)"); 

        b.rec_size=result1;

        match Auth::new(&b) {
            Err(e) => assert!(false, "got error {}", e),
            Ok(x) => assert!(x.is_some(), "check auth object exists"),
        }


    }
}



