use std::{collections::{HashSet, LinkedList}, ops::Deref, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}};
use libc::close;
use once_cell::sync::Lazy;
use crate::{ae::{create_file_event, delete_file_event, el::Mask, handler::{read_query_from_client, send_reply_to_client}}, anet::{nonblock, tcp_no_delay}, redis::{cmd::lookup_command, server_read, server_write}, util::{log, timestamp, LogLevel}, zmalloc::used_memory};
use super::{cmd::{call, MultiCmd, MAX_SIZE_INLINE_CMD}, obj::{RedisObject, StringStorageType, CRLF}, RedisDB, ReplState};


/// 
/// Redis Server-side client state.
/// 


/// Client state.
pub static CLIENTS: Lazy<Box<RwLock<LinkedList<Arc<RwLock<RedisClient>>>>>> = Lazy::new(|| {
    Box::new(RwLock::new(LinkedList::new()))
});
pub fn clients_read() -> RwLockReadGuard<'static, LinkedList<Arc<RwLock<RedisClient>>>> {
    CLIENTS.read().unwrap()
}
pub fn clients_write() -> RwLockWriteGuard<'static, LinkedList<Arc<RwLock<RedisClient>>>> {
    CLIENTS.write().unwrap()
}
/// Deleted client fd set.
pub static DELETED_CLIENTS: Lazy<RwLock<HashSet<i32>>> = Lazy::new(|| {
    RwLock::new(HashSet::new())
});
pub fn deleled_clients_read() -> RwLockReadGuard<'static, HashSet<i32>> {
    DELETED_CLIENTS.read().unwrap()
}
pub fn deleted_clients_write() -> RwLockWriteGuard<'static, HashSet<i32>> {
    DELETED_CLIENTS.write().unwrap()
}


/// With multiplexing we need to take per-clinet state.
/// Clients are taken in a liked list.
pub struct RedisClient {
    fd: i32,
    pub db: Option<Arc<RwLock<RedisDB>>>,
    pub query_buf: String,
    pub argv: Vec<Arc<RedisObject>>,
    mbargv: Vec<Arc<RedisObject>>,
    bulk_len: i32,                  // bulk read len. -1 if not in bulk read mode
    multi_bulk: i32,                // multi bulk command format active
    pub sent_len: usize,
    pub reply: LinkedList<Arc<RedisObject>>,
    pub flags: ClientFlags,
    pub last_interaction: u64,          // time of the last interaction, used for timeout (in seconds)
    authenticated: bool,            // when requirepass is non-NULL
    repl_state: ReplState,          // replication state if this is a slave
    mstate: MultiState,             // MULTI/EXEC state
    blocking_keys: Vec<Arc<RedisObject>>,   // The key we are waiting to terminate a blocking
                                            // operation such as BLPOP. Otherwise NULL.
    io_keys: LinkedList<Arc<RedisObject>>,  // Keys this client is waiting to be loaded from the
                                            // swap file in order to continue.
}

impl RedisClient {
    pub fn create(fd: i32) -> Result<Arc<RwLock<RedisClient>>, String> {
        match nonblock(fd) {
            Ok(_) => {},
            Err(e) => { return Err(e); },
        }
        match tcp_no_delay(fd) {
            Ok(_) => {},
            Err(e) => { return Err(e); },
        }
        let mut c = RedisClient {
            fd,
            db: None,
            query_buf: String::new(),
            argv: Vec::new(),
            bulk_len: -1,
            multi_bulk: 0,
            mbargv: Vec::new(),
            sent_len: 0,
            flags: ClientFlags(0),
            last_interaction: timestamp().as_secs(),
            authenticated: false,
            repl_state: ReplState::None,
            reply: LinkedList::new(),
            blocking_keys: Vec::new(),
            mstate: MultiState { commands: Vec::new() },
            io_keys: LinkedList::new(),
        };
        c.select_db(0);
        let c = Arc::new(RwLock::new(c));
        create_file_event(fd, Mask::Readable, Arc::new(read_query_from_client))?;
        clients_write().push_back(c.clone());
        Ok(c)
    }

    /// In Redis commands are always executed in the context of a client, so in
    /// order to load the append only file we need to create a fake client.
    pub fn create_fake_client() -> RedisClient {
        let mut c = RedisClient { 
            db: None, 
            fd: -1, 
            query_buf: String::new(),
            argv: Vec::new(),
            flags: ClientFlags(0),
            // We set the fake client as a slave waiting for the synchronization
            // so that Redis will not try to send replies to this client.
            repl_state: ReplState::WaitBgSaveStart,
            reply: LinkedList::new(),
            mbargv: Vec::new(),
            bulk_len: 0,
            multi_bulk: 0,
            sent_len: 0,
            last_interaction: 0,
            authenticated: false,
            mstate: MultiState { commands: Vec::new() },
            blocking_keys: Vec::new(),
            io_keys: LinkedList::new(),
        };

        c.select_db(0);
        c
    }

    pub fn process_input_buf(&mut self) {
        // Before to process the input buffer, make sure the client is not
        // waitig for a blocking operation such as BLPOP. Note that the first
        // iteration the client is never blocked, otherwise the processInputBuffer
        // would not be called at all, but after the execution of the first commands
        // in the input buffer the client may be blocked, and the "goto again"
        // will try to reiterate. The following line will make it return asap.
        if self.flags.is_blocked() || self.flags.is_io_wait() {
            return;
        }
        // log(LogLevel::Verbose, &format!("process_input_buf entered: {}", self.bulk_len));
        if self.bulk_len == -1 {
            if self.query_buf.contains("\n") {
                // Read the first line of the query
                let query_buf_c = self.query_buf.clone();
                let mut iter = query_buf_c.lines();
                let query = iter.next().expect("first query doesn't exist");
                let remaining: Vec<&str> = iter.collect();
                self.query_buf = remaining.join("\r\n");
                if query_buf_c.ends_with("\n") && !self.query_buf.is_empty() {
                    self.query_buf.push_str("\r\n");
                }

                // Now we can split the query in arguments
                let argv: Vec<Arc<RedisObject>> = query.split(" ")
                    .filter(|a| !a.is_empty())
                    .map(|a| Arc::new(RedisObject::String { ptr: StringStorageType::String(a.to_string()) }))
                    .collect();
                self.argv = argv;
                if !self.argv.is_empty() {
                    // log(LogLevel::Verbose, "process_input_buf ing");
                    // Execute the command. If the client is still valid
                    // after processCommand() return and there is something
                    // on the query buffer try to process the next command.
                    if self.process_command() && !self.query_buf.is_empty() {
                        self.process_input_buf();
                    }
                } else {
                    // Nothing to process, argc == 0. Just process the query
                    // buffer if it's not empty or return to the caller
                    if !self.query_buf.is_empty() {
                        self.process_input_buf();
                    }
                }
                return;
            } else if self.query_buf.len() >= MAX_SIZE_INLINE_CMD {
                log(LogLevel::Verbose, "Client protocol error");
                // TODO: free client?
                return;
            }
        } else {
            // Bulk read handling. Note that if we are at this point
            // the client already sent a command terminated with a newline,
            // we are reading the bulk data that is actually the last
            // argument of the command.
            if self.bulk_len as usize <= self.query_buf.len() {
                let query_buf_c = self.query_buf.clone();
                let mut iter = query_buf_c.lines();
                let arg = iter.next().expect("last arg doesn't exist");
                if arg.len() != self.bulk_len as usize {
                    log(LogLevel::Warning, &format!("arg '{}' isn't consistent with bulk len '{}'", arg, self.bulk_len));
                    // TODO: free client?
                    return;
                }
                let remaining: Vec<&str> = iter.collect();
                self.query_buf = remaining.join("\r\n");
                if query_buf_c.ends_with("\n") && !self.query_buf.is_empty() {
                    self.query_buf.push_str("\r\n");
                }

                self.argv.push(Arc::new(RedisObject::String { ptr: StringStorageType::String(arg.to_string()) }));

                // Process the command. If the client is still valid after
                // the processing and there is more data in the buffer
                // try to parse it.
                if self.process_command() && !self.query_buf.is_empty() {
                    self.process_input_buf();
                }
            }
        }
    }

    /// If this function gets called we already read a whole
    /// command, argments are in the client argv/argc fields.
    /// processCommand() execute the command or prepare the
    /// server for a bulk read from the client.
    /// 
    /// If 1 is returned the client is still alive and valid and
    /// and other operations can be performed by the caller. Otherwise
    /// if 0 is returned the client was destroied (i.e. after QUIT).
    fn process_command(&mut self) -> bool {
        // log(LogLevel::Verbose, "process_command");
        // Free some memory if needed (maxmemory setting)
        if server_read().max_memory > 0 {
            server_write().free_memory_if_needed();
        }
        
        // Handle the multi bulk command type. This is an alternative protocol
        // supported by Redis in order to receive commands that are composed of
        // multiple binary-safe "bulk" arguments. The latency of processing is
        // a bit higher but this allows things like multi-sets, so if this
        // protocol is used only for MSET and similar commands this is a big win.
        if self.multi_bulk == 0 && 
            self.argv.len() == 1 && 
            self.argv[0].deref().string().is_some() &&
            self.argv[0].deref().string().unwrap().string().is_some() &&
            self.argv[0].deref().string().unwrap().string().unwrap().starts_with("*") {
            
            let mbulk = self.argv[0].deref().string().unwrap().string().unwrap();
            match mbulk[1..].parse() {
                Ok(n) => { self.multi_bulk = n; },
                Err(e) => {
                    log(LogLevel::Warning, &format!("Parsing multi bulk '{}' failed: {}", mbulk, e));
                },
            }

            if self.multi_bulk <= 0 {
                self.reset();
                return true;
            } else {
                self.argv.pop();
                return true;
            }
        } else if self.multi_bulk != 0 {
            if self.bulk_len == -1 {
                let bulk = self.argv[0].deref().string().unwrap().string().unwrap();
                if bulk.starts_with("$") {
                    match bulk[1..].parse() {
                        Ok(n) => { self.bulk_len = n; },
                        Err(e) => {
                            log(LogLevel::Warning, &format!("Parsing bulk '{}' failed: {}", bulk, e));
                        },
                    }
                    self.argv.clear();
                    if self.bulk_len < 0 || self.bulk_len > 1024*1024*1024 {
                        self.add_reply_str("-ERR invalid bulk write count\r\n");
                        self.reset();
                        return true;
                    }
                    return true;
                } else {
                    self.add_reply_str("-ERR multi bulk protocol error\r\n");
                    self.reset();
                    return true;
                }
            } else {
                let bulk_arg = self.argv.pop().unwrap();
                self.mbargv.push(bulk_arg);
                self.multi_bulk -= 1;
                if self.multi_bulk == 0 {
                    // Here we need to swap the multi-bulk argc/argv with the
                    // normal argc/argv of the client structure.
                    (self.argv, self.mbargv) = (self.mbargv.clone(), self.argv.clone());

                    // We need to set bulklen to something different than -1
                    // in order for the code below to process the command without
                    // to try to read the last argument of a bulk command as
                    // a special argument.
                    self.bulk_len = 0;
                    // continue below and process the command
                } else {
                    self.bulk_len = -1;
                    return true;
                }
            }
        }
        // -- end of multi bulk commands processing --

        let name_arg = self.argv[0].clone();
        let name = name_arg.string().unwrap().string().unwrap();
        // The QUIT command is handled as a special case. Normal command
        // procs are unable to close the client connection safely
        if name.eq_ignore_ascii_case("quit") {
            deleted_clients_write().insert(self.fd);
            return false;
        }

        // Now lookup the command and check ASAP about trivial error conditions
        // such wrong arity, bad command name and so forth.
        let cmd = lookup_command(name);
        match cmd {
            None => {
                self.add_reply_str(&format!("-ERR unknown command '{}'\r\n", name));
                self.reset();
                return true;
            },
            Some(cmd) => {
                if (cmd.arity() > 0 && cmd.arity() != self.argv.len() as i32) ||
                    (self.argv.len() as i32) < (-cmd.arity()) {    // TODO: < 0???
                    self.add_reply_str(&format!("-ERR wrong number of arguments for '{}' command\r\n", cmd.name()));
                    self.reset();
                    return true;
                } else if server_read().max_memory > 0 && 
                    cmd.flags().is_deny_oom() &&
                    used_memory() > server_read().max_memory {
                    self.add_reply_str("-ERR command not allowed when used memory > 'maxmemory'\r\n");
                    self.reset();
                    return true;
                } else if cmd.flags().is_bulk() && self.bulk_len == -1 {
                    // This is a bulk command, we have to read the last argument yet.
                    let last_arg = self.argv.pop().unwrap();
                    let bulk = last_arg.string().unwrap().string().unwrap();
                    match bulk.parse() {
                        Ok(n) => { self.bulk_len = n; },
                        Err(e) => {
                            log(LogLevel::Warning, &format!("Parsing bulk '{}' failed: {}", bulk, e));
                        },
                    }

                    if self.bulk_len < 0 || self.bulk_len > 1024*1024*1024 {
                        self.add_reply_str("-ERR invalid bulk write count\r\n");
                        self.reset();
                        return true;
                    }
                    // It is possible that the bulk read is already in the
                    // buffer. Check this condition and handle it accordingly.
                    // This is just a fast path, alternative to call processInputBuffer().
                    // It's a good idea since the code is small and this condition
                    // happens most of the times.
                    if self.query_buf.len() as i32 >= self.bulk_len {
                        let query_buf_c = self.query_buf.clone();
                        let mut iter = query_buf_c.lines();
                        let arg = iter.next().expect("bulk arg doesn't exist");
                        let remaining: Vec<&str> = iter.collect();
                        self.query_buf = remaining.join("\r\n");
                        if query_buf_c.ends_with("\n") && !self.query_buf.is_empty() {
                            self.query_buf.push_str("\r\n");
                        }

                        self.argv.push(Arc::new(RedisObject::String { ptr: StringStorageType::String(arg.to_string()) }));
                    } else {
                        // Otherwise return... there is to read the last argument
                        // from the socket.
                        return true;
                    }
                }

                // Let's try to share objects on the command arguments vector
                // TODO

                // Let's try to encode the bulk object to save space.
                // TODO

                // Check if the user is authenticated
                // TODO

                let exec = lookup_command("exec").unwrap();
                let discard = lookup_command("discard").unwrap();
                // Exec the command
                if self.flags.is_multi() && !Arc::ptr_eq(&cmd.proc(), &exec.proc()) &&
                    !Arc::ptr_eq(&cmd.proc(), &discard.proc()) {
                        // TODO
                } else {
                    // TODO: vm
                    call(self, cmd);
                }

                // Prepare the client for the next command
                self.reset();
                return true;
            },
        };
    }

    pub fn add_reply(&mut self, obj: Arc<RedisObject>) {
        if self.reply.is_empty() &&
            (self.repl_state == ReplState::None ||
             self.repl_state == ReplState::Online) &&
            create_file_event(self.fd, Mask::Writable, 
                Arc::new(send_reply_to_client)).is_err() {
            return;
        }

        // TODO: vm related

        self.reply.push_back(Arc::new(obj.get_decoded()));
    }

    pub fn add_reply_bulk(&mut self, obj: Arc<RedisObject>) {
        self.add_reply_bulk_len(obj.clone());
        self.add_reply(obj);
        self.add_reply(CRLF.clone());
    }

    fn add_reply_bulk_len(&mut self, obj: Arc<RedisObject>) {
        let mut len = 0usize;
        match obj.deref() {
            RedisObject::String { ptr } => {
                match ptr {
                    StringStorageType::String(s) => { len = s.len(); },
                    StringStorageType::Integer(n) => {
                        // Compute how many bytes will take this integer as a radix 10 string
                        len = n.to_string().len();
                    },
                }
            }
            _ => {
                todo!()
            },
        }
        self.add_reply_str(&format!("${len}\r\n"));
    }

    fn add_reply_str(&mut self, s: &str) {
        self.add_reply(Arc::new(RedisObject::String { ptr: StringStorageType::String(s.to_string()) }));
    }

    pub fn lookup_key_read_or_reply(&mut self, key: &str, obj: Arc<RedisObject>) -> Option<Arc<RedisObject>> {
        let db = self.db.clone().expect("db doesn't exist");
        let db_r = db.read().unwrap();
        match db_r.dict.get(key) {
            None => {
                self.add_reply(obj);
                None
            },
            Some(v) => { Some(v.clone()) },
        }
    }

    fn select_db(&mut self, id: i32) {
        if id < 0 || id >= server_read().dbnum {
            log(LogLevel::Warning, &format!("Invalid db #{} out of [0, {})", id, server_read().dbnum));
            return;
        }
        self.db = Some(server_read().dbs[id as usize].clone());
    }

    /// reset prepare the client to process the next command
    fn reset(&mut self) {
        self.argv.clear();
        self.mbargv.clear();
        self.bulk_len = -1;
        self.multi_bulk = 0;
    }

    pub fn fd(&self) -> i32 {
        self.fd
    }
    pub fn set_argv(&mut self, argv: Vec<Arc<RedisObject>>) {
        self.argv = argv;
    }
}

impl Drop for RedisClient {
    fn drop(&mut self) {
        // Note that if the client we are freeing is blocked into a blocking
        // call, we have to set querybuf to NULL *before* to call
        // unblockClientWaitingData() to avoid processInputBuffer() will get
        // called. Also it is important to remove the file events after
        // this, because this call adds the READABLE event.
        // TODO: blocked

        delete_file_event(self.fd, Mask::Readable);
        delete_file_event(self.fd, Mask::Writable);
        unsafe { close(self.fd); }

        // Remove from the list of clients waiting for swapped keys
        // TODO

        // Other cleanup
        if self.flags.is_slave() {
            // TODO
        }
        if self.flags.is_master() {
            server_write().master = None;
            server_write().repl_state = ReplState::Connect;
        }
    }
}


pub struct MultiState {
    commands: Vec<MultiCmd>,    // Array of MULTI commands
}


pub struct ClientFlags(u8);
impl ClientFlags {
    /// This client is a slave server
    fn slave() -> ClientFlags {
        ClientFlags(1)
    }
    /// This client is a master server
    fn master() -> ClientFlags {
        ClientFlags(2)
    }
    /// This client is a slave monitor, see MONITOR
    fn monitor() -> ClientFlags {
        ClientFlags(4)
    }
    /// This client is in a MULTI context
    fn multi() -> ClientFlags {
        ClientFlags(8)
    }
    /// The client is waiting in a blocking operation
    fn blocked() -> ClientFlags {
        ClientFlags(16)
    }
    /// The client is waiting for Virtual Memory I/O
    fn io_wait() -> ClientFlags {
        ClientFlags(32)
    }
    pub fn is_slave(&self) -> bool {
        (self.0 & Self::slave().0) != 0
    }
    pub fn is_master(&self) -> bool {
        (self.0 & Self::master().0) != 0
    }
    pub fn is_blocked(&self) -> bool {
        (self.0 & Self::blocked().0) != 0
    }
    fn is_io_wait(&self) -> bool {
        (self.0 & Self::io_wait().0) != 0
    }
    fn is_multi(&self) -> bool {
        (self.0 & Self::multi().0) != 0
    }
}

