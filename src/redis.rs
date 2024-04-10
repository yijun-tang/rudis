use libc::off_t;

use crate::{ae::{BeforeSleepProc, EventLoop}, util::{timestamp, LogLevel}};

pub static REDIS_VERSION: &str = "1.3.7";
static MAX_IDLE_TIME: i32 = 60 * 5;             // default client timeout
static DEFAULT_DBNUM: i32 = 16;
static SERVER_PORT: u16 = 6379;

// Hashes related defaults
static HASH_MAX_ZIPMAP_ENTRIES: usize = 64;
static HASH_MAX_ZIPMAP_VALUE: usize = 512;

struct SaveParam {
    seconds: u128,
    changes: i32,
}

enum AppendFsync {
    No,
    Always,
    EverySec,
}

/// Slave replication state - slave side
enum ReplState {
    None,       // No active replication
    Connect,    // Must connect to master
    Connected,  // Connected to master
}

/// With multiplexing we need to take per-clinet state.
/// Clients are taken in a liked list.
struct RedisClient {

}

pub struct RedisServer {
    port: u16,
    sharing_pool_size: u32,
    el: Box<EventLoop>,
    // Configuration
    verbosity: LogLevel,
    glue_output_buf: i32,
    max_idle_time: i32,
    dbnum: i32,
    daemonize: bool,
    append_only: bool,
    append_fsync: AppendFsync,
    last_fsync: u128,
    append_fd: i32,
    append_sel_db: i32,
    pid_file: &'static str,
    save_param: Vec<SaveParam>,
    log_file: &'static str,
    bind_addr: &'static str,
    db_filename: &'static str,
    append_filename: &'static str,
    require_pass: &'static str,
    share_objects: bool,
    rdb_compression: bool,
    // Replication related
    is_slave: bool,
    master_auth: &'static str,
    master_host: &'static str,
    master_port: u16,
    master: Option<Box<RedisClient>>,       // client that is master for this slave
    repl_state: ReplState,

    max_clients: u32,
    max_memory: u128,
    blpop_blocked_clients: u32,
    vm_blocked_clients: u32,
    // Virtual memory configuration
    vm_enabled: bool,
    vm_swap_file: &'static str,
    vm_page_size: off_t,
    vm_pages: off_t,
    vm_max_memory: u128,
    // Hashes config
    hash_max_zipmap_entries: usize,
    hash_max_zipmap_value: usize,

    vm_max_threads: i32,                            // Max number of I/O threads running at the same time
}

impl RedisServer {
    pub fn new() -> RedisServer {
        let save_param = vec![
            SaveParam { seconds: 60 * 60, changes: 1 },             // save after 1 hour and 1 change
            SaveParam { seconds: 300, changes: 100 },               // save after 5 minutes and 100 changes
            SaveParam { seconds: 60, changes: 10000 },              // save after 1 minute and 10000 changes
        ];
        RedisServer { 
            port: SERVER_PORT, 
            el: EventLoop::create().unwrap(),   // TODO
            verbosity: LogLevel::Verbose,
            max_idle_time: MAX_IDLE_TIME,
            dbnum: DEFAULT_DBNUM,
            save_param,
            log_file: "",                       // "" = log on standard output
            bind_addr: "",
            glue_output_buf: 1,
            daemonize: false,
            append_only: false,
            append_fsync: AppendFsync::Always,
            last_fsync: timestamp().as_nanos(),
            append_fd: -1,
            append_sel_db: -1,                  // Make sure the first time will not match
            pid_file: "/var/run/redis.pid",
            db_filename: "dump.rdb",
            append_filename: "appendonly.aof",
            require_pass: "",
            share_objects: false,
            rdb_compression: true,
            sharing_pool_size: 1024,
            max_clients: 0,
            blpop_blocked_clients: 0,
            max_memory: 0,
            vm_enabled: false,
            vm_swap_file: "/tmp/redis-%p.vm",
            vm_page_size: 256,                  // 256 bytes per page
            vm_pages: 1024 * 1024 * 100,        // 104 millions of pages
            vm_max_memory: 1024 * 1024 * 1024,  // 1 GB of RAM
            vm_max_threads: 4,
            vm_blocked_clients: 0,
            hash_max_zipmap_entries: HASH_MAX_ZIPMAP_ENTRIES,
            hash_max_zipmap_value: HASH_MAX_ZIPMAP_VALUE,

            // Replication related
            is_slave: false,
            master_auth: "",
            master_host: "",
            master_port: 6379,
            master: None,
            repl_state: ReplState::None,
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
