use std::{collections::{HashMap, LinkedList}, env::set_current_dir, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Read, Write}, process::{exit, id}, ptr::null_mut, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}};
use libc::{close, dup2, fclose, fopen, fork, fprintf, getpid, open, pid_t, setsid, signal, FILE, O_RDWR, SIGHUP, SIGPIPE, SIG_IGN, STDERR_FILENO, STDIN_FILENO, STDOUT_FILENO};
use once_cell::sync::Lazy;
use crate::{client::RedisClient, eventloop::{create_file_event, create_time_event, Mask}, handler::{accept_handler, server_cron}, net::tcp_server, obj::RedisObject, util::{log, oom, timestamp, yes_no_to_bool, LogLevel}};


/// 
/// Redis Server.
///  


pub const IO_BUF_LEN: usize = 1024;
pub static ONE_GB: i32 = 1024 * 1024 * 1024;
static MAX_IDLE_TIME: i32 = 60 * 5;             // default client timeout
static DEFAULT_DBNUM: i32 = 16;
static SERVER_PORT: u16 = 6379;

// Hashes related defaults
static HASH_MAX_ZIPMAP_ENTRIES: usize = 64;
static HASH_MAX_ZIPMAP_VALUE: usize = 512;


/// Redis Server state.
/// 
pub static SERVER: Lazy<Arc<RwLock<RedisServer>>> = Lazy::new(|| { Arc::new(RwLock::new(RedisServer::new())) });
pub fn server_read() -> RwLockReadGuard<'static, RedisServer> {
    SERVER.read().unwrap()
}
pub fn server_write() -> RwLockWriteGuard<'static, RedisServer> {
    SERVER.write().unwrap()
}

pub struct RedisServer {
    port: u16,
    pub fd: i32,
    pub dbs: Vec<Arc<RwLock<RedisDB>>>,
    sharing_pool: HashMap<Arc<RedisObject>, usize>,      // Pool used for object sharing
    sharing_pool_size: u32,
    pub dirty: u128,                                // changes to DB from the last save
    slaves: LinkedList<Arc<RwLock<RedisClient>>>,
    monitors: LinkedList<RedisClient>,
    cron_loops: i32,                                            // number of times the cron function run
    pub last_save: u64,                                             // Unix time of last save succeeded (in seconds)
    // Fields used only for stats
    stat_starttime: u64,                        // server start time (in seconds)
    pub stat_numcommands: u128,                     // number of processed commands
    stat_numconnections: u128,                  // number of connections received
    // Configuration
    verbosity: LogLevel,
    glue_output_buf: bool,
    pub max_idle_time: i32,
    pub dbnum: i32,
    pub daemonize: bool,
    pub append_only: bool,
    pub append_fsync: AppendFsync,
    pub append_file: Option<File>,
    pub last_fsync: u64,
    pub append_sel_db: i32,
    pub pid_file: String,
    pub bg_save_child_pid: pid_t,
    pub bg_rewrite_child_pid: pid_t,
    pub bg_rewrite_buf: String,                     // buffer taken by parent during oppend only rewrite
    save_params: Vec<SaveParam>,
    log_file: String,
    bind_addr: String,
    pub db_filename: String,
    pub append_filename: String,
    pub require_pass: String,
    pub share_objects: bool,
    pub rdb_compression: bool,
    // Replication related
    is_slave: bool,
    master_auth: String,
    master_host: String,
    master_port: u16,
    pub master: Option<Arc<RedisClient>>,       // client that is master for this slave
    pub repl_state: ReplState,

    max_clients: u32,
    pub max_memory: u128,
    pub blpop_blocked_clients: u32,
    // Hashes config
    hash_max_zipmap_entries: usize,
    hash_max_zipmap_value: usize,

    // Virtual memory state
    devnull: Option<Arc<dyn Write + Sync + Send>>,
}
impl RedisServer {
    pub fn new() -> RedisServer {
        let save_params = vec![
            SaveParam { seconds: 60 * 60, changes: 1 },             // save after 1 hour and 1 change
            SaveParam { seconds: 300, changes: 100 },               // save after 5 minutes and 100 changes
            SaveParam { seconds: 60, changes: 10000 },              // save after 1 minute and 10000 changes
        ];
        RedisServer { 
            port: SERVER_PORT, 
            fd: -1,
            dbs: Vec::with_capacity(DEFAULT_DBNUM as usize),
            sharing_pool: HashMap::new(),
            dirty: 0,
            slaves: LinkedList::new(),
            monitors: LinkedList::new(),
            cron_loops: 0,
            last_save: timestamp().as_secs(),
            stat_starttime: timestamp().as_secs(),
            stat_numcommands: 0,
            stat_numconnections: 0,
            verbosity: LogLevel::Verbose,
            max_idle_time: MAX_IDLE_TIME,
            dbnum: DEFAULT_DBNUM,
            save_params,
            log_file: String::new(),                       // "" = log on standard output
            bind_addr: String::new(),
            glue_output_buf: true,
            daemonize: false,
            append_only: false,
            append_fsync: AppendFsync::Always,
            append_file: None,
            last_fsync: timestamp().as_secs(),
            append_sel_db: -1,                  // Make sure the first time will not match
            pid_file: "/var/run/redis.pid".to_string(),
            bg_save_child_pid: -1,
            bg_rewrite_child_pid: -1,
            bg_rewrite_buf: String::new(),
            db_filename: "dump.rdb".to_string(),
            append_filename: "appendonly.aof".to_string(),
            require_pass: String::new(),
            share_objects: false,
            rdb_compression: true,
            sharing_pool_size: 1024,
            max_clients: 0,
            blpop_blocked_clients: 0,
            max_memory: 0,
            hash_max_zipmap_entries: HASH_MAX_ZIPMAP_ENTRIES,
            hash_max_zipmap_value: HASH_MAX_ZIPMAP_VALUE,

            // Replication related
            is_slave: false,
            master_auth: String::new(),
            master_host: String::new(),
            master_port: 6379,
            master: None,
            repl_state: ReplState::None,
            devnull: None,
        }
    }

    pub fn init_server(&mut self) {
        unsafe {
            // ignore handler
            signal(SIGHUP, SIG_IGN);
            signal(SIGPIPE, SIG_IGN);
        }

        match OpenOptions::new().write(true).open("/dev/null") {
            Ok(f) => { self.devnull = Some(Arc::new(f)); },
            Err(e) => {
                log(LogLevel::Warning, &format!("Can't open /dev/null: {}", e));
                exit(1);
            },
        }

        match tcp_server(self.port, &self.bind_addr) {
            Ok(fd) => { self.fd = fd; },
            Err(e) => {
                log(LogLevel::Warning, &format!("Opening TCP port: {}", e));
                exit(1);
            },
        }

        for i in 0..self.dbnum {
            self.dbs.push(Arc::new(RwLock::new(RedisDB::new(i))));
        }

        create_time_event(1, Arc::new(server_cron), None, None);
        match create_file_event(self.fd, Mask::Readable, Arc::new(accept_handler)) {
            Ok(_) => {},
            Err(e) => { oom(&e); },    // TODO: is it appropriate to call oom?
        }

        if self.append_only {
            match OpenOptions::new().write(true).append(true).create(true).open(&self.append_filename) {
                Ok(f) => { self.append_file = Some(f); },
                Err(e) => {
                    log(LogLevel::Warning, &format!("Can't open the append-only file: {}", e));
                    exit(1);
                },
            }
        }
    }

    /// I agree, this is a very rudimental way to load a configuration...
    /// will improve later if the config gets more complex
    pub fn load_server_config(&mut self, filename: &str) {
        let mut _reader: Option<Box<dyn Read>> = None;
        let mut line_num = 0;

        if filename.eq("-") {
            _reader = Some(Box::new(io::stdin()));
        } else {
            if let Ok(f) = File::open(filename) {
                _reader = Some(Box::new(f));
            } else {
                log(LogLevel::Warning, "Fatal error, can't open config file");
                exit(1);
            }
        }

        let load_err = |err: &str, line: &str, line_num: i32| {
            eprintln!("*** FATAL CONFIG FILE ERROR ***");
            eprintln!("Reading the configuration file, at line {line_num}");
            eprintln!(">>> '{line}'");
            eprintln!("{err}");
            exit(1);
        };
        
        let buf_reader = BufReader::new(_reader.unwrap());
        for line in buf_reader.lines() {
            if let Ok(line) = line {
                line_num += 1;
                let trimed_line = line.trim();

                // Skip comments and blank lines
                if trimed_line.starts_with("#") || trimed_line.is_empty() {
                    continue;
                }

                // Split into arguments
                let argv: Vec<&str> = trimed_line.split_whitespace().collect();
                let argc = argv.len();

                // Execute config directives
                match &argv[0].to_ascii_lowercase()[..] {
                    "timeout" if argc == 2 => {
                        let mut err = String::new();
                        match argv[1].parse() {
                            Ok(t) => { self.max_idle_time = t; },
                            Err(e) => { err = e.to_string(); },
                        }
                        if self.max_idle_time < 0 {
                            err = "Invalid timeout value".to_string();
                        }
                        if !err.is_empty() { load_err(&err, trimed_line, line_num); }
                    },
                    "port" if argc == 2 => {
                        let mut err = String::new();
                        match argv[1].parse() {
                            Ok(p) => { self.port = p; },
                            Err(e) => { err = e.to_string(); },
                        }
                        if self.port < 1 {
                            err = "Invalid port".to_string();
                        }
                        if !err.is_empty() { load_err(&err, trimed_line, line_num); }
                    },
                    "bind" if argc == 2 => { self.bind_addr = argv[1].to_string(); },
                    "save" if argc == 3 => {
                        let mut err = String::new();
                        let s: Result<u64, _> = argv[1].parse();
                        let c: Result<i32, _> = argv[2].parse();
                        match (s, c) {
                            (Ok(seconds), Ok(changes)) => {
                                if seconds < 1 || changes < 0 {
                                    err = "Invalid save parameters".to_string();
                                } else {
                                    self.append_server_save_params(seconds, changes); 
                                }
                            },
                            _ => { err = "seconds or changes parsing failed".to_string(); }
                        }
                        if !err.is_empty() { load_err(&err, trimed_line, line_num); }
                    },
                    "dir" if argc == 2 => {
                        match set_current_dir(argv[1]) {
                            Ok(_) => {},
                            Err(e) => {
                                log(LogLevel::Warning, &format!("Can't chdir to '{}': {}", argv[1], e));
                                exit(1);
                            },
                        };
                    },
                    "loglevel" if argc == 2 => {
                        match &argv[1].to_ascii_lowercase()[..] {
                            "debug" => { self.verbosity = LogLevel::Debug; },
                            "verbose" => { self.verbosity = LogLevel::Verbose; },
                            "notice" => { self.verbosity = LogLevel::Notice; },
                            "warning" => { self.verbosity = LogLevel::Warning; },
                            _ => {
                                let err = "Invalid log level. Must be one of debug, verbose, notice or warning";
                                load_err(err, &line, line_num);
                            },
                        }
                    },
                    "logfile" if argc == 2 => {
                        match &argv[1].to_ascii_lowercase()[..] {
                            "stdout" => { self.log_file = String::new(); },
                            filename if !filename.is_empty() => {
                                // Test if we are able to open the file. The server will not
                                // be able to abort just for this problem later...
                                match OpenOptions::new().append(true).open(filename) {
                                    Ok(_) => {},
                                    Err(e) => {
                                        let err = format!("Can't open the log file: {}", e);
                                        load_err(&err, &line, line_num);
                                    },
                                }
                            },
                            _ => { load_err("logfile can't be empty", &line, line_num); },
                        }
                    },
                    "databases" if argc == 2 => {
                        let mut err = String::new();
                        match argv[1].parse() {
                            Ok(n) => { self.dbnum = n; },
                            Err(e) => { err = e.to_string(); },
                        }
                        if self.dbnum < 1 {
                            err = "Invalid number of databases".to_string();
                        }
                        if !err.is_empty() { load_err(&err, trimed_line, line_num); }
                    },
                    "include" if argc == 2 => { self.load_server_config(argv[1]); },
                    "maxclients" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(m_c) => { self.max_clients = m_c; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    "maxmemory" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(m_m) => { self.max_memory = m_m; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    "slaveof" if argc == 3 => {
                        self.master_host = argv[1].to_string();
                        match argv[2].parse() {
                            Ok(m_p) => { self.master_port = m_p; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                        self.repl_state = ReplState::Connect;
                    },
                    "masterauth" if argc == 2 => { self.master_auth = argv[1].to_string(); },
                    "glueoutputbuf" if argc == 2 => {
                        match yes_no_to_bool(argv[1]) {
                            Ok(b) => { self.glue_output_buf = b; },
                            Err(e) => { load_err(&e, trimed_line, line_num); },
                        }
                    },
                    "shareobjects" if argc == 2 => {
                        match yes_no_to_bool(argv[1]) {
                            Ok(b) => { self.share_objects = b; },
                            Err(e) => { load_err(&e, trimed_line, line_num); },
                        }
                    },
                    "rdbcompression" if argc == 2 => {
                        match yes_no_to_bool(argv[1]) {
                            Ok(b) => { self.rdb_compression = b; },
                            Err(e) => { load_err(&e, trimed_line, line_num); },
                        }
                    },
                    "shareobjectspoolsize" if argc == 2 => {
                        let mut err = String::new();
                        match argv[1].parse() {
                            Ok(sp_size) => { self.sharing_pool_size = sp_size; },
                            Err(e) => { err = e.to_string(); },
                        }
                        if self.sharing_pool_size < 1 {
                            err = "invalid object sharing pool size".to_string();
                        }
                        if !err.is_empty() { load_err(&err, trimed_line, line_num); }
                    },
                    "daemonize" if argc == 2 => {
                        match yes_no_to_bool(argv[1]) {
                            Ok(b) => { self.daemonize = b; },
                            Err(e) => { load_err(&e, trimed_line, line_num); },
                        }
                    },
                    "appendonly" if argc == 2 => {
                        match yes_no_to_bool(argv[1]) {
                            Ok(b) => { self.append_only = b; },
                            Err(e) => { load_err(&e, trimed_line, line_num); },
                        }
                    },
                    "appendfsync" if argc == 2 => {
                        match &argv[1].to_ascii_lowercase()[..] {
                            "no" => { self.append_fsync = AppendFsync::No; },
                            "always" => { self.append_fsync = AppendFsync::Always; },
                            "everysec" => { self.append_fsync = AppendFsync::EverySec; },
                            _ => { load_err("argument must be 'no', 'always' or 'everysec'", &line, line_num); },
                        }
                    },
                    "requirepass" if argc == 2 => { self.require_pass = argv[1].to_string(); },
                    "pidfile" if argc == 2 => { self.pid_file = argv[1].to_string(); },
                    "dbfilename" if argc == 2 => { self.db_filename = argv[1].to_string(); },
                    "hash-max-zipmap-entries" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(mz_e) => { self.hash_max_zipmap_entries = mz_e; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    "hash-max-zipmap-value" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(mz_v) => { self.hash_max_zipmap_value = mz_v; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    _ => {
                        let err = "Bad directive or wrong number of arguments";
                        load_err(err, &line, line_num);
                    },
                }
            } else {
                let err = "Directive parsing failed";
                load_err(err, "", line_num);
            }
        }
    }

    pub fn daemonize(&self) {
        let mut _fd = -1;
        let mut _fp: *mut FILE = null_mut();
        unsafe {
            if fork() != 0 { exit(0); }     // parent exits
            setsid();                               // create a new session
    
            // Every output goes to /dev/null. If Redis is daemonized but
            // the 'logfile' is set to 'stdout' in the configuration file
            // it will not log at all.
            _fd = open("/dev/null".as_ptr() as *const i8, O_RDWR, 0);
            if _fd != -1 {
                dup2(_fd, STDIN_FILENO);
                dup2(_fd, STDOUT_FILENO);
                dup2(_fd, STDERR_FILENO);
                if _fd > STDERR_FILENO { close(_fd); }
            }
    
            // Try to write the pid file
            _fp = fopen(self.pid_file.as_ptr() as *const i8, "w".as_ptr() as *const i8);
            if !_fp.is_null() {
                fprintf(_fp, "%d\n".as_ptr() as *const i8, getpid());
                fclose(_fp);
            }
        }
    }

    /// This function gets called when 'maxmemory' is set on the config file to limit
    /// the max memory used by the server, and we are out of memory.
    /// This function will try to, in order:
    /// 
    /// - Free objects from the free list
    /// - Try to remove keys with an EXPIRE set
    /// 
    /// It is not possible to free enough memory to reach used-memory < maxmemory
    /// the server will start refusing commands that will enlarge even more the
    /// memory usage.
    pub fn free_memory_if_needed(&mut self) {
        // TODO
        log(LogLevel::Warning, "free memory if needed!!!");
    }

    pub fn clear(&mut self) -> u128 {
        let mut removed = 0u128;
        for db in &self.dbs {
            let mut db_w = db.write().unwrap();
            removed += db_w.dict.len() as u128;
            db_w.dict.clear();
            db_w.expires.clear();
        }
        removed
    }

    
    pub fn reset_server_save_params(&mut self) {
        self.save_params.clear();
    }
    fn append_server_save_params(&mut self, seconds: u64, changes: i32) {
        self.save_params.push(SaveParam { seconds, changes });
    }
    pub fn save_params(&self) -> &Vec<SaveParam> {
        &self.save_params
    }

    pub fn dirty(&self) -> u128 {
        self.dirty
    }
    pub fn last_save(&self) -> u64 {
        self.last_save
    }
    pub fn log_file(&self) -> &str {
        &self.log_file
    }
    pub fn verbosity(&self) -> &LogLevel {
        &self.verbosity
    }
    pub fn cron_loops(&self) -> i32 {
        self.cron_loops
    }
    pub fn set_cron_loops(&mut self, c: i32) {
        self.cron_loops = c;
    }
    pub fn dbnum(&self) -> i32 {
        self.dbnum
    }
    pub fn dbs(&self) -> &Vec<Arc<RwLock<RedisDB>>> {
        &self.dbs
    }
    pub fn bg_save_child_pid(&self) -> i32 {
        self.bg_save_child_pid
    }
    pub fn bg_rewrite_child_pid(&self) -> i32 {
        self.bg_rewrite_child_pid
    }
    pub fn max_clients(&self) -> u32 {
        self.max_clients
    }
    pub fn stat_numconnections(&self) -> u128 {
        self.stat_numconnections
    }
    pub fn set_stat_numconnections(&mut self, s: u128) {
        self.stat_numconnections = s;
    }
    pub fn slaves(&self) -> &LinkedList<Arc<RwLock<RedisClient>>> {
        &self.slaves
    }
    pub fn sharing_pool(&self) -> &HashMap<Arc<RedisObject>, usize> {
        &self.sharing_pool
    }
    pub fn is_daemonize(&self) -> bool {
        self.daemonize
    }
    pub fn append_only(&self) -> bool {
        self.append_only
    }
    pub fn append_filename(&self) -> &str {
        &self.append_filename
    }
    pub fn db_filename(&self) -> &str {
        &self.db_filename
    }
    pub fn port(&self) -> u16 {
        self.port
    }

    #[cfg(target_os = "linux")]
    pub fn linux_overcommit_memory_warning(&self) {
        if self.linux_overcommit_memory_value() == 0 {
            log(LogLevel::Warning, "WARNING overcommit_memory is set to 0! Background save may fail under low condition memory. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
        }
    }
    #[cfg(target_os = "linux")]
    fn linux_overcommit_memory_value(&self) -> i32 {
        use std::io::{BufRead, BufReader, Read};

        let mut _reader: Option<Box<dyn Read>> = None;
        match OpenOptions::new().read(true).open("/proc/sys/vm/overcommit_memory") {
            Ok(f) => { _reader = Some(Box::new(f)); },
            Err(e) => {
                log(LogLevel::Warning, &format!("Can't open '/proc/sys/vm/overcommit_memory' file: {}", e));
                return -1;
            },
        }
        let mut buf = String::new();
        match BufReader::new(_reader.unwrap()).read_line(&mut buf) {
            Ok(_) => {
                match buf.trim().parse() {
                    Ok(r) => r,
                    Err(e) => {
                        log(LogLevel::Warning, &format!("Parsing '{}' as i32 failed: {}", buf, e));
                        -1
                    },
                }
            },
            Err(e) => {
                log(LogLevel::Warning, &format!("Reading '/proc/sys/vm/overcommit_memory' file failed: {}", e));
                -1
            },
        }
    }
}


pub struct RedisDB {
    pub dict: HashMap<String, Arc<RwLock<RedisObject>>>,                                        // The keyspace for this DB
    pub expires: HashMap<String, u64>,                                                  // Timeout of keys with a timeout set
    pub blocking_keys: HashMap<String, Arc<LinkedList<Arc<RwLock<RedisClient>>>>>,      // Keys with clients waiting for data (BLPOP)
    pub id: i32,
}
impl RedisDB {
    pub fn new(id: i32) -> RedisDB {
        Self { dict: HashMap::new(), expires: HashMap::new(), blocking_keys: HashMap::new(), id }
    }
}


#[derive(PartialEq)]
pub enum ReplState {
    // Slave replication state - slave side
    None,       // No active replication
    Connect,    // Must connect to master
    Connected,  // Connected to master
    // Slave replication state - from the point of view of master
    // Note that in SEND_BULK and ONLINE state the slave receives new updates
    // in its output queue. In the WAIT_BGSAVE state instead the server is waiting
    // to start the next background saving in order to send updates to it.
    WaitBgSaveStart,        // master waits bgsave to start feeding it
    WaitBgSaveEnd,          // master waits bgsave to start bulk DB transmission
    SendBulk,               // master is sending the bulk DB
    Online,                 // bulk DB already transmitted, receive updates
}


pub struct SaveParam {
    seconds: u64,
    changes: i32,
}
impl SaveParam {
    pub fn seconds(&self) -> u64 {
        self.seconds
    }
    pub fn changes(&self) -> i32 {
        self.changes
    }
}


#[derive(PartialEq)]
pub enum AppendFsync {
    No,
    Always,
    EverySec,
}


static REDIS_VERSION: &str = "1.3.7";
pub fn print_logo() {
    log(LogLevel::Notice, &format!("                _._                                                  "));
    log(LogLevel::Notice, &format!("           _.-``__ ''-._                                             "));
    log(LogLevel::Notice, &format!("      _.-``    `.  `_.  ''-._           Rudis {}", REDIS_VERSION));
    log(LogLevel::Notice, &format!("  .-`` .-```.  ```\\/    _.,_ ''-._                                   "));
    log(LogLevel::Notice, &format!(" (    '      ,       .-`  | `,    )     Re-implementation in Rust!"));
    log(LogLevel::Notice, &format!(" |`-._`-...-` __...-.``-._|'` _.-'|     Port: {}", server_read().port()));
    log(LogLevel::Notice, &format!(" |    `-._   `._    /     _.-'    |     PID: {}", id()));
    log(LogLevel::Notice, &format!("  `-._    `-._  `-./  _.-'    _.-'                                   "));
    log(LogLevel::Notice, &format!(" |`-._`-._    `-.__.-'    _.-'_.-'|                                  "));
    log(LogLevel::Notice, &format!(" |    `-._`-._        _.-'_.-'    |     Just for Learning Purpose!   "));
    log(LogLevel::Notice, &format!("  `-._    `-._`-.__.-'_.-'    _.-'                                   "));
    log(LogLevel::Notice, &format!(" |`-._`-._    `-.__.-'    _.-'_.-'|                                  "));
    log(LogLevel::Notice, &format!(" |    `-._`-._        _.-'_.-'    |                                  "));
    log(LogLevel::Notice, &format!("  `-._    `-._`-.__.-'_.-'    _.-'                                   "));
    log(LogLevel::Notice, &format!("      `-._    `-.__.-'    _.-'                                       "));
    log(LogLevel::Notice, &format!("          `-._        _.-'                                           "));
    log(LogLevel::Notice, &format!("              `-.__.-'                                               "));
}


#[cfg(test)]
mod tests {
    use std::io::{BufRead, Cursor};

    #[test]
    fn char_test() {
        assert!('\t'.is_whitespace());
        assert!('\r'.is_whitespace());
        assert!('\n'.is_whitespace());
        assert!(' '.is_whitespace());
    }

    #[test]
    fn cfg_file_line_test() {
        let text = "\n\n\n\n".to_string();
        let cursor = Cursor::new(text);
        let lines: Vec<String> = cursor.lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines.len(), 4);
    }
}

