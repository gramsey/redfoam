use std::fmt::{Display, Formatter};
//use std::convert;
use std::io;

pub enum Er {
    BadAuth,
    NotReady,
    ClientTcpRead(io::Error),
    IsClosed,
    InvalidSequence,
    CantReadFile(io::Error),
}

impl Display for Er {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s: String;

        let message = match self {
            Er::BadAuth => "Incorrect Authentication information supplied - connection will be closed",
            Er::NotReady => "Still reading input - not enough data to proceed",
            Er::ClientTcpRead(e) => {
                s = format!("Failed to read tcp data for this client, io error :{}", e);
                s.as_str()
            }
            Er::IsClosed => "Tried to process client that is already closed",
            Er::InvalidSequence => "Sequence number on incoming request from client is invalid",
            Er::CantReadFile(e) => {
                s = format!("Failed to read topic file :{}", e);
                s.as_str()
            }
        };
        f.write_str(message)
    }
}

