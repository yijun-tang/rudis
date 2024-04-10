use crate::ae::{BeforeSleepProc, EventLoop};

pub static REDIS_VERSION: &str = "1.3.7";

pub struct RedisServer {
    daemonize: bool,
    append_only: bool,
    append_filename: &'static str,
    db_filename: &'static str,
    port: u16,
    el: Box<EventLoop>,
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer { 
            daemonize: false, 
            append_only: false, 
            append_filename: "", 
            db_filename: "", 
            port: 6379, 
            el: EventLoop::create().unwrap(),   // TODO
        }
    }
    pub fn daemonize(&self) -> bool {
        self.daemonize
    }

    pub fn append_only(&self) -> bool {
        self.append_only
    }

    pub fn append_filename(&self) -> &str {
        self.append_filename
    }

    pub fn db_filename(&self) -> &str {
        self.db_filename
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn set_before_sleep_proc(&mut self, before_sleep: Option<BeforeSleepProc>) {
        self.el.set_before_sleep_proc(before_sleep);
    }

    pub fn main(&mut self) {
        self.el.main();
    }
}

pub fn init_server_config() {

}

pub fn reset_server_save_params() {

}

pub fn load_server_config(filename: &str) {

}

pub fn daemonize() {

}

pub fn init_server() {

}

pub fn load_append_only_file(filename: &str) -> Result<(), String> {
    todo!()
}

pub fn rdb_load(filename: &str) -> Result<(), String> {
    todo!()
}

pub fn before_sleep(el: &mut EventLoop) {

}
