use super::RedisServer;

impl RedisServer {
    pub fn init_vm(&mut self) {
        todo!()
    }

    pub fn swap_one_object_blocking(&self) -> Result<(), String> {
        self.swap_one_object(false)
    }
    
    fn swap_one_object(&self, use_threads: bool) -> Result<(), String> {
        todo!()
    }
}
