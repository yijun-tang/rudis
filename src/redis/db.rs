use std::{collections::HashMap, sync::Arc};

use super::obj::RedisObject;


pub struct RedisDB {
    pub dict: HashMap<String, Arc<RedisObject>>,              // The keyspace for this DB
    pub expires: HashMap<String, String>,           // Timeout of keys with a timeout set
    blocking_keys: HashMap<String, String>,     // Keys with clients waiting for data (BLPOP)
    io_keys: Option<HashMap<String, String>>,   // Keys with clients waiting for VM I/O
    id: i32,
}

impl RedisDB {
    pub fn new(vm_enabled: bool, id: i32) -> RedisDB {
        let mut io_keys: Option<HashMap<String, String>> = None;
        if vm_enabled {
            io_keys = Some(HashMap::new());
        }
        RedisDB { dict: HashMap::new(), expires: HashMap::new(), blocking_keys: HashMap::new(), io_keys, id }
    }
}
