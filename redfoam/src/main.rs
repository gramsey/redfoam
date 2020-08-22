use redfoam::tcp;
use std::env;

fn main() {
    println!("start");

    let addr = env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:9090".to_string());

    tcp::run_server(addr);
    tcp::run_consumer_server("127.0.0.1:9091".to_string());
}

