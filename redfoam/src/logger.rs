#[macro_export]
macro_rules! trace {
        ($msg:expr) => {
            #[cfg(debug_assertions)] print!("TRACE : ");
            #[cfg(debug_assertions)] println!($msg);
        };
        ($($msg:expr),*) => {
            #[cfg(debug_assertions)] print!("TRACE : ");
            #[cfg(debug_assertions)] println!($($msg,)*);
        };
}
macro_rules! debug {
        ($msg:expr) => {
            #[cfg(debug_assertions)] print!("DEBUG : ");
            #[cfg(debug_assertions)] println!($msg);
        };
        ($($msg:expr),*) => {
            #[cfg(debug_assertions)] print!("DEBUG : ");
            #[cfg(debug_assertions)] println!($($msg,)*);
        };
}
/*
macro_rules! info {
        ($expression:expr) => {
            println!($expression);
        };
}
macro_rules! warn {
        ($expression:expr) => {
            println!($expression);
        };
}
macro_rules! error {
        ($expression:expr) => {
            println!($expression);
        };
}
*/
