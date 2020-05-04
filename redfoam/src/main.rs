use redfoam::tcp;
use std::env;

fn main() {
    let addr = env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:9090".to_string());

    tcp::run_server(addr);
}

