pub mod datasource;
pub mod exec;
pub mod task;
pub mod model;
pub mod db;
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;
pub mod core;
pub mod macros;
pub mod util;

#[macro_use]
extern crate lazy_static;

use crate::core::error::Error;


pub type Json = serde_json::Value;
pub type Result<T> = std::result::Result<T, Error>;


pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
