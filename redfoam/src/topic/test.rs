use super::*;
use super::super::tcp::{RecordType};
use super::super::test_support::TestEnvironment;
use super::super::test_support::TestTopic;

use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use std::fs;

#[test]
fn send_file() {
    let mut file = std::fs::File::create("/tmp/data.txt").expect("create failed");
    file.write_all("Hello World".as_bytes()).expect("write failed");

    let file_read = std::fs::File::open("/tmp/data.txt").expect("create failed");

    let addr = "127.0.0.1:34293"; 

    let listener = TcpListener::bind(&addr).unwrap();
    let mut client_stream = TcpStream::connect(&addr).unwrap();
    client_stream.set_read_timeout(Some(Duration::new(1, 0))).expect("cant set timeout duration on client");

    let server_stream = listener.incoming().next().unwrap().unwrap();

    let f = file_read.as_raw_fd();
    let s = server_stream.as_raw_fd();
    trace!("sending file");

    Topic::linux_send_file (s, f, None, false).expect("error on sendfile");

    let mut buffer1 = String::new();
    client_stream.read_to_string(&mut buffer1).expect("error on read_to_string");
    assert_eq!(buffer1, "Hello World", "testing result 1 (full string no update)");

    Topic::linux_send_file (s, f, Some(5), true);
    let mut buffer3 = String::new();
    client_stream.read_to_string(&mut buffer3);
    assert_eq!(buffer3, "Hello", "testing result 3 (5 bytes with update");

    Topic::linux_send_file (s, f, None, true);
    let mut buffer4 = String::new();
    client_stream.read_to_string(&mut buffer4);
    assert_eq!(buffer4, " World", "testing result 4 (rest of file from pos of last update");


}

#[test]
fn send_follower() {
    let env = TestEnvironment::new("send_follower");
    let mut t = Topic::test_new(&env, 2, "test2", false);
    let mut t_producer = t.test_open(true);

    t_producer.write(b"hello");


    // set up tcp client 
    let addr = "127.0.0.1:34292"; 

    let listener = TcpListener::bind(&addr).unwrap();
    let mut client_stream = TcpStream::connect(&addr).unwrap();
    client_stream.set_read_timeout(Some(Duration::new(1, 0)));

    let server_stream = listener.incoming().next().unwrap().unwrap();

    let c = ConsumerClient::new(1, server_stream);

    let mut client_list : HashMap<u32, ConsumerClient> = HashMap::new();
    client_list.insert(1, c);

    // run test
    t.follow(1);
    let r = t.send_followers(&mut client_list, RecordType::DataFeed); 

    match r {
        Err(e) => assert!(false, "check sendfile 1 worked {}", e),
        Ok(Some(n)) => assert_eq!(n , 5,  "checking at lest 5 bytes written"),
        Ok(None) => assert!(false, "nothing written to stream"),
    }

    trace!("start read as client");
    let mut buffer = String::new();
    client_stream.read_to_string(&mut buffer);
    assert_eq!(String::from("hello"), buffer, "strings don't match");

    t_producer.write(b"world");

    let r2 = t.send_followers(&mut client_list, RecordType::DataFeed); 

    match r2 {
        Err(e) => assert!(false, "check sendfile 1 worked {}", e),
        Ok(Some(n)) => assert_eq!(n , 5,  "checking at lest 5 bytes written"),
        Ok(None) => assert!(false, "nothing written to stream"),
    }

    let mut buffer2 = String::new();
    client_stream.read_to_string(&mut buffer2);
    assert_eq!(String::from("world"), buffer2, "strings don't match");

}

#[test]
fn latest_file() {
    fs::create_dir("/tmp/testx");
    File::create("/tmp/testx/d0000000000000000");
    File::create("/tmp/testx/i0000000000000000");
    File::create("/tmp/testx/d000000000000001c");
    File::create("/tmp/testx/i000000000000001c");
    File::create("/tmp/testx/d00000000000000a3");
    File::create("/tmp/testx/i00000000000000a3");

    let config = TopicConfig {
        topic_id : 1,
        topic_name : String::from("testx"),
        folder : String::from("/tmp"),
        replication : 0, 
        file_mask : 8,
    };

    let latest_data_name = Topic::latest_file_name('d', &config);
    assert!(latest_data_name.is_ok(), "trying to get latest index file without error"); 
    assert_eq!(latest_data_name.unwrap(), "/tmp/testx/d00000000000000a3", "trying to get latest data file");

    let latest_index_name = Topic::latest_file_name('i', &config);
    assert!(latest_index_name.is_ok(), "trying to get latest index file without error");
    assert_eq!(latest_index_name.unwrap(), "/tmp/testx/i00000000000000a3", "trying to get latest index file");
}

#[test]
fn test_topicwrite() {
    let env = TestEnvironment::new("topicwrite_env");
    let mut t = Topic::test_new(&env, 1, "test", true);

    let idx = t.index;
    let written : usize = t.write(&b"hello"[..]).expect("trying to write to file");
    assert_eq!(written, 5, "5 bytes should be written to file");
    t.end_rec();
    assert_eq!(t.index, idx + 1, "checking index is incremented by one");
}

#[test]
fn test_readfirstrecord() {
    let env = TestEnvironment::new("readfirstrecord");
    let mut t = Topic::test_new(&env, 1, "test1", false);

    let content = b"First Record";
    t.create_entries(content, 2, 2);

    let mut buf : [u8; 8] = [0;8];

    match t.read_index_into(&mut buf, 0) {
        Err(e) => assert!(false, "error reading index {}", e),
        Ok(n) => assert_eq!(n, 8, "check 8 index bytes read"),
    }

    let idx = u64::from_le_bytes(buf);
    trace!("read index {}", idx);

    assert_ne!(idx, 0, "check index value is not zero");

    let mut databuf : [u8;1024] = [0;1024];
    match t.read_data_into(&mut databuf[..idx as usize], 0) {
        Err(e) => assert!(false, "error reading data {}", e),
        Ok(n) => assert_eq!(n, idx as usize, "check {} data bytes read", n),
    }

    assert_eq!(databuf[0], content[0], "check first char");
    assert_eq!(databuf[11], content[11], "check last char");
}

#[test]
#[ignore]
fn test_topiclist() {
    let mut tl_producer = TopicList::init(true).unwrap();
    let mut tl_consumer = TopicList::init(false).unwrap();

    let p_topic_result = tl_producer.topic_for_id(1);

    assert!(p_topic_result.is_ok(), "topic 1 should be in topic list");
    let p_topic = p_topic_result.unwrap();

    let write_result = p_topic.write(&b"TopicList test message"[..]);
    assert!(write_result.is_ok(),  "problem while writing to topic");

    assert_eq!(write_result.unwrap(), 22 as usize);

    let end_rec_result = p_topic.end_rec();
    assert!(end_rec_result.is_ok(),  "problem while writing to topic for end record");

    let mut buffer = [0; 1024];
    let mut events = tl_consumer.notify.read_events(&mut buffer)
        .expect("Error while reading events");

    /* first event for writing to data file */
    match events.next() {
        Some(e) => {
            match tl_consumer.watchers.get(&e.wd) {
                Some(topic_id) => assert_eq!(*topic_id, 1u32),
                None => assert!(false, "topic id missing from watcher list"),
            }
            match e.name {
                Some(name) => {
                    assert_eq!(name.to_str().unwrap().chars().nth(0), Some('d'));
                }, 
                None => assert!(false, "event should have a file name"),
            }
        },
        None => assert!(false, "missing event from inotify"),
    }

    /* second event for writing to index file */
    match events.next() {
        Some(e) => {
            match tl_consumer.watchers.get(&e.wd) {
                Some(topic_id) => assert_eq!(*topic_id, 1u32),
                None => assert!(false, "topic id missing from watcher list"),
            }
            match e.name {
                Some(name) => {
                    assert_eq!(name.to_str().unwrap().chars().nth(0), Some('i'));
                }, 
                None => assert!(false, "event should have a file name"),
            }
        },
        None => assert!(false, "missing event from inotify"),
    }
}

