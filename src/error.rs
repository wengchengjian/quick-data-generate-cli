use thiserror::Error;




pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    
    #[error("Other error: {}", _0)]
    Other(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    
    #[error("Input/output error: {}", _0)]
    Io(#[source] IoError),
}

/// This type enumerates IO errors.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("Input/output error: {}", _0)]
    Io(#[source] std::io::Error),
    
    #[error("arg:`{0}` not found")]
    ArgNotFound(String),
    
    #[error("parse schema error")]
    ParseSchemaError,
    
    #[error("unkown data type:`{0}`")]
    UnkownTypeError(String),
}


impl From<IoError> for Error {
    fn from(io: IoError) -> Self {
        Error::Io(io)
    }
}

impl From<std::io::Error> for IoError {
    fn from(err: std::io::Error) -> Self {
        IoError::Io(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.into())
    }
}