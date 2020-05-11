use std::str;

use super::buff::Buff;
use super::er::Er;

pub struct Auth {
    //client_id : u32,
}
impl Auth {
    pub fn new(buff : &Buff) -> Result<Self, Er> {

        if buff.is_end_of_record() {

            let content = str::from_utf8(buff.data()).unwrap();
            println!("content : {} ", content);

            let mut authpart = content.split(";");

            let topic_name = String::from(authpart.next().unwrap());
            println!("topic_name : {}", topic_name);


            let auth_token = authpart.next().unwrap();
            println!("auth_token : {}", auth_token);

            if auth_token == "ANON" {
                Ok(Auth {})
            } else {
                Err(Er::BadAuth)
            }
        } else {
            Err(Er::NotReady)
        }
    }
}
