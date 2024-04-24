use std::{fs::OpenOptions, io::Read};
use crate::util::{log, LogLevel};
use super::RedisServer;

impl RedisServer {
    pub fn rdb_load(&self) -> Result<(), String> {
        let mut reader: Option<Box<dyn Read>> = None;
        match OpenOptions::new().read(true).open(&self.db_filename) {
            Ok(f) => {},
            Err(e) => {
                log(LogLevel::Warning, &format!("Fatal error: can't open the rdb file for reading: {}", e));
                return Err(e.to_string());
            },
        }
        todo!()
    }

    pub fn rdb_save(&self) {
        // TODO
    }
}
