
#[macro_export]
macro_rules! trace {
        ($msg:expr) => {
            #[cfg(debug_assertions)] let trace_current_time = std::time::SystemTime::now();
            #[cfg(debug_assertions)] let trace_timestamp = trace_current_time.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards");
            #[cfg(debug_assertions)] print!("{:?}", trace_timestamp);
            #[cfg(debug_assertions)] print!("\u{001B}[32mTRACE [{}:{}] : ", file!(), line!());
            #[cfg(debug_assertions)] println!($msg);
            #[cfg(debug_assertions)] print!("\u{001B}[0m");
        };
        ($($msg:expr),*) => {
            #[cfg(debug_assertions)] let trace_current_time = std::time::SystemTime::now();
            #[cfg(debug_assertions)] let trace_timestamp = trace_current_time.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards");
            #[cfg(debug_assertions)] print!("{:?}", trace_timestamp);
            #[cfg(debug_assertions)] print!("\u{001B}[32mTRACE [{}:{}] : ", file!(), line!());
            #[cfg(debug_assertions)] println!($($msg,)*);
            #[cfg(debug_assertions)] print!("\u{001B}[0m");
        };
}

#[macro_export]
macro_rules! log_error {
        ($msg:expr) => {
            let trace_current_time = std::time::SystemTime::now();
            let trace_timestamp = trace_current_time.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards");
            print!("{:?}", trace_timestamp);
            print!("\u{001B}[31mERROR [{}:{}] : ", file!(), line!());
            println!($msg);
            print!("\u{001B}[0m");
        };
        ($($msg:expr),*) => {
            let trace_current_time = std::time::SystemTime::now();
            let trace_timestamp = trace_current_time.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards");
            print!("{:?}", trace_timestamp);
            print!("\u{001B}[31mERROR [{}:{}] : ", file!(), line!());
            println!($($msg,)*);
            print!("\u{001B}[0m");
        };
}
