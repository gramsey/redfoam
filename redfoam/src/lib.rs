
mod producer {
    use tokio::io::AsyncRead;
    use std::io::{Error, ErrorKind};

    struct Topic {}

    impl Topic {
        fn Get (name : &String) -> Result<Topic, Error> {
            Err(Error::new(ErrorKind::Other, "function not written yet"))
        }

        fn Write (input : impl Send + AsyncRead) -> Result<(), Error> {
            Err(Error::new(ErrorKind::Other, "function not written yet"))
        }
    }

}
