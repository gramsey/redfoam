//use std::fmt::{Display, Formatter, Result};
//use std::convert;
use std::io;

pub enum Er {
    BadAuth,
    NotReady,
    ClientTcpRead(io::Error),
}

// impl Display for Er {
//    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
//        write!(f, "(, )")
//    }
//}

