use std::fmt::{Display, Formatter};
//use std::convert;
use std::io;
use std::sync::mpsc;
use std::num;

#[derive(Debug)]
pub enum Er {
    BadAuth,
    NotReady,
    ClientTcpRead(io::Error),
    ClientTcpWrite(io::Error),
    ServerTcpRead(io::Error),
    ServerTcpWrite(io::Error),
    IsClosed,
    InvalidSequence,
    CantReadFile(io::Error),
    CantWriteFile(io::Error),
    CantReadDir(io::Error),
    CantOpenFile(io::Error),
    InotifyError(io::Error),
    FailedToReturnMessage(mpsc::SendError<Vec<u8>>),
    NoConsumerStart,
    FailedToReadDataStart,
    TopicNotFound,
    IsNone,
    InvalidEventMask,
    BadFileName,
    BadOffset(String, num::ParseIntError),
}

impl Display for Er {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s: String;

        let message = match self {
            Er::BadAuth => "Incorrect Authentication information supplied - connection will be closed",
            Er::NotReady => "Still reading input - not enough data to proceed",
            Er::ClientTcpRead(e) => {
                s = format!("Failed to read tcp data client->server, io error :{}", e);
                s.as_str()
            },
            Er::ClientTcpWrite(e) => {
                s = format!("Failed to write tcp data client->server, io error :{}", e);
                s.as_str()
            },
            Er::ServerTcpRead(e) => {
                s = format!("Failed to read tcp data server->client, io error :{}", e);
                s.as_str()
            },
            Er::ServerTcpWrite(e) => {
                s = format!("Failed to write tcp data server->client, io error :{}", e);
                s.as_str()
            },
            Er::IsClosed => "Tried to process client that is already closed",
            Er::InvalidSequence => "Sequence number on incoming request from client is invalid",
            Er::InotifyError(e) => {
                s = format!("Problem trying to use inotify (linux) : {}", e);
                s.as_str()
            },
            Er::CantReadFile(e) => {
                s = format!("Failed to read topic file :{}", e);
                s.as_str()
            },
            Er::CantWriteFile(e) => {
                s = format!("Failed to write to topic file :{}", e);
                s.as_str()
            },
            Er::CantReadDir(e) => {
                s = format!("Failed to read topic directory :{}", e);
                s.as_str()
            },
            Er::CantOpenFile(e) => {
                s = format!("Failed to open topic file :{}", e);
                s.as_str()
            },
            Er::FailedToReturnMessage(e) => {
                s = format!("Failed to read topic file :{}", e);
                s.as_str()
            },
            Er::NoConsumerStart => "Recieved content from server, but never recieved the header message containing start references.",
            Er::FailedToReadDataStart => "Could not read expected start value from consumer header message",
            Er::TopicNotFound => "Trying to retrieve topic - not found in collection",
            Er::BadFileName => "Trying to parse a file for topic - bad filename",
            Er::IsNone => "Value returned as 'None' but this should not be possible",
            Er::InvalidEventMask => "Event mask returned from event is unexpected",
            Er::BadOffset(f_name, e) => {
                s = format!("Bad topic filename {} - cannot parse the hex offset value :{}", f_name, e);
                s.as_str()
            },
        };
        f.write_str(message)
    }
}

