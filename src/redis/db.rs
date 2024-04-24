use std::{collections::{HashMap, LinkedList}, sync::{Arc, RwLock}};

use super::{client::RedisClient, obj::RedisObject};


pub struct RedisDB {
    pub dict: HashMap<String, Arc<RwLock<RedisObject>>>,                                        // The keyspace for this DB
    pub expires: HashMap<String, u64>,                                                  // Timeout of keys with a timeout set
    pub blocking_keys: HashMap<String, Arc<LinkedList<Arc<RwLock<RedisClient>>>>>,      // Keys with clients waiting for data (BLPOP)
    io_keys: Option<HashMap<String, String>>,   // Keys with clients waiting for VM I/O
    pub id: i32,
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
