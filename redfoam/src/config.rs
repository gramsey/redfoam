use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub node_id : u32,
    pub topics : Vec<TopicConfig>,
}

impl Config {
    pub fn new() -> Config {
        let config_string: &str = "node_id = 0\n[[topics]]\ntopic_id = 1\ntopic_name = \"test\"\nreplication = 0\nfolder=\"/tmp\"\nfile_mask=4";
        toml::from_str(config_string).unwrap()
    }
}

#[derive(Deserialize, Debug)]
pub struct TopicConfig {
    pub topic_id : u32,
    pub topic_name : String,
    pub folder : String,
    pub replication : u8,
    pub file_mask : u8, // 16 - how many hex digits in filename, that is 2^(file_mask*4) = number of records in single file
}

#[test]
fn test_config() {
    let config_string: &str = "node_id = 0\n[[topics]]\ntopic_id = 1\ntopic_name = \"test\"\nreplication = 0\nfolder=\"/tmp\"\nfile_mask=4";
    let config: Config = toml::from_str(config_string).unwrap();

    assert_eq!(config.node_id, 0);
    let topics : Vec<TopicConfig> = config.topics;
    let t: &TopicConfig =  &topics[0];
    assert_eq!(t.topic_id, 1);
    assert_eq!(t.topic_name, "test");
    assert_eq!(t.replication, 0);
    assert_eq!(t.folder, "/tmp");
}
