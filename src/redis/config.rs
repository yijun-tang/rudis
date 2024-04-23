use std::{env::set_current_dir, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Read}, process::exit};
use crate::util::{log, yes_no_to_bool, LogLevel};
use super::{AppendFsync, RedisServer, ReplState};


///
/// Redis Configuration Parsing.
///  


impl RedisServer {
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
                        let s: Result<u128, _> = argv[1].parse();
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
                    "vm-enabled" if argc == 2 => {
                        match yes_no_to_bool(argv[1]) {
                            Ok(b) => { self.vm_enabled = b; },
                            Err(e) => { load_err(&e, trimed_line, line_num); },
                        }
                    },
                    "vm-swap-file" if argc == 2 => { self.vm_swap_file = argv[1].to_string(); },
                    "vm-max-memory" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(m_m) => { self.vm_max_memory = m_m; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    "vm-page-size" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(p_s) => { self.vm_page_size = p_s; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    "vm-pages" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(p) => { self.vm_pages = p; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
                    "vm-max-threads" if argc == 2 => {
                        match argv[1].parse() {
                            Ok(m_t) => { self.vm_max_threads = m_t; },
                            Err(e) => { load_err(&e.to_string(), trimed_line, line_num); },
                        }
                    },
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
}
