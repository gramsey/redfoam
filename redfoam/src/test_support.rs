use super::*;

use std::net::{TcpListener};
use std::thread::sleep;
use std::time::Duration;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Write};
use rand::{thread_rng, Rng};
use std::convert::TryInto;


pub struct TestEnvironment {
    pub folder : String,
}

impl TestEnvironment {

    pub fn new( name:&str ) -> TestEnvironment {
        let dir_name = format!("/tmp/redfoam_{}", name);
        fs::create_dir(&dir_name).expect("create topic dir failed");
        TestEnvironment { folder : dir_name}
    }
}

pub trait TestTopic {
    fn test_new(env:&TestEnvironment, id:u32, name:&str) -> Self;
    fn create_entries(&self, content:&[u8], max_repeat:u32, record_count:u32) ;
}

impl TestTopic for topic::Topic {

    fn test_new(env:&TestEnvironment, id:u32, name:&str) -> Self {
        let dir_name = format!("{}/{}", &env.folder, name);
        let data_file_name = format!("{}/{}/d0000000000000000", env.folder, name);
        let index_file_name = format!("{}/{}/i0000000000000000", env.folder, name);

        let mut folder = fs::create_dir(dir_name).expect("create topic dir failed");
        let mut index_file = File::create(index_file_name).expect("create topic index file failed");
        let mut data_file = File::create(data_file_name).expect("create topic data file failed");

        let topic_config = config::TopicConfig {
            topic_id : id,
            topic_name : String::from(name),
            folder : env.folder.clone(),
            replication : 0,
            file_mask : 0,
        };

        topic::Topic::open(topic_config, false).expect("trying to create topic")
    }

    fn create_entries(&self, content:&[u8], repeat_limit:u32, record_count:u32) {
        let mut rng = thread_rng();
        let mut file_opener = OpenOptions::new();
        file_opener.append(true);
        let mut index_file = file_opener.open(self.get_index_file_name()).expect("create topic index file failed");
        let mut data_file = file_opener.open(self.get_data_file_name()).expect("create topic data file failed");

        let mut position:u64 = 0;
        for i in 0..record_count {

            let n: u32 = rng.gen_range(1, repeat_limit);
            for j in 0..n {
                data_file.write_all(content).expect("failed writing test data");
                position += content.len() as u64;
            }
            trace!("position {}", position);
            index_file.write_all(&position.to_le_bytes()).expect("failed wrting index with test data");
        }
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.folder);
    }
}

mod test {
    use super::*;
    use std::path::Path;
    use super::TestTopic;

    #[test] 
    fn test_env() {

        {
            let te = TestEnvironment::new("mytest");
            assert!(Path::new("/tmp/redfoam_mytest").exists());

        }
        assert!(!Path::new("/tmp/redfoam_mytest").exists()); //ensure cleaned up
    }

    #[test]
    fn test_testtopic() {
        let te = TestEnvironment::new("testtopic");
        let t = topic::Topic::test_new(&te, 1, "mytesttopic");
        assert!(Path::new("/tmp/redfoam_testtopic/mytesttopic").exists());
        assert!(Path::new("/tmp/redfoam_testtopic/mytesttopic/d0000000000000000").exists());
        assert!(Path::new("/tmp/redfoam_testtopic/mytesttopic/i0000000000000000").exists());

        let content = b":START:Some junk content blahdebalah barghasldkfj :END:";
        t.create_entries(content, 10, 10);

        let alldata_result = fs::read_to_string("/tmp/redfoam_testtopic/mytesttopic/d0000000000000000");
        assert!(alldata_result.is_ok(), "Error trying to read from data file");
        let alldata = alldata_result.unwrap();

        let min_size = content.len() * 10;

        assert!(alldata.len() >= min_size, "data file seems too small at {}", alldata.len());
        assert!(alldata.starts_with(":START:"), "data file doesn't start with same start marker");
        assert!(alldata.ends_with(":END:"), "data file doesn't end with same end marker");

        let index_result = fs::read("/tmp/redfoam_testtopic/mytesttopic/i0000000000000000");

        assert!(index_result.is_ok(), "can't read index file");
        let index = index_result.unwrap();

        assert_eq!(index.len(), 80, "should have 10 * 8 bytes of index entries, 8 bytes (u64) for each record");

        let last_index = u64::from_le_bytes(index[72..80].try_into().unwrap());
        assert_eq!(last_index, alldata.len() as u64, "last index value should match data file size");


    }

}

