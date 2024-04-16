use std::{borrow::BorrowMut, collections::LinkedList, ops::Deref, sync::{Arc, RwLock}};
use libc::{c_void, read, strerror, EAGAIN};
use crate::{ae::{el::el_write, EventLoop, Mask}, anet::{nonblock, tcp_no_delay}, redis::{cmd::{discard_command, exec_command, lookup_command}, server_read, server_write, IO_BUF_LEN}, util::{error, log, timestamp, LogLevel}, zmalloc::used_memory};
use super::{cmd::{call, RedisCommand, MAX_SIZE_INLINE_CMD}, obj::{RedisObject, StringStorageType}, RedisDB, ReplState};

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

    fn is_blocked(&self) -> bool {
        (self.0 & Self::blocked().0) != 0
    }

    fn is_io_wait(&self) -> bool {
        (self.0 & Self::io_wait().0) != 0
    }

    fn is_multi(&self) -> bool {
        (self.0 & Self::multi().0) != 0
    }
}

/// Client MULTI/EXEC state
pub struct MultiCmd {
    argv: Vec<Arc<RedisObject>>,
    cmd: RedisCommand,
}

pub struct MultiState {
    commands: Vec<MultiCmd>,    // Array of MULTI commands
}

/// With multiplexing we need to take per-clinet state.
/// Clients are taken in a liked list.
pub struct RedisClient {
    fd: i32,
    db: Option<Arc<RedisDB>>,
    query_buf: String,
    argv: Vec<Arc<RedisObject>>,
    mbargv: Vec<Arc<RedisObject>>,
    bulk_len: i32,                  // bulk read len. -1 if not in bulk read mode
    multi_bulk: i32,                // multi bulk command format active
    sent_len: usize,
    reply: LinkedList<Arc<RedisObject>>,
    flags: ClientFlags,
    last_interaction: u64,          // time of the last interaction, used for timeout (in seconds)
    authenticated: bool,            // when requirepass is non-NULL
    repl_state: ReplState,          // replication state if this is a slave
    mstate: MultiState,             // MULTI/EXEC state
    blocking_keys: Vec<Arc<RedisObject>>,   // The key we are waiting to terminate a blocking
                                            // operation such as BLPOP. Otherwise NULL.
    io_keys: LinkedList<Arc<RedisObject>>,  // Keys this client is waiting to be loaded from the
                                            // swap file in order to continue.
}

impl Drop for RedisClient {
    fn drop(&mut self) {
        todo!()
    }
}

impl RedisClient {
    pub fn fd(&self) -> i32 {
        self.fd
    }

    pub fn create(el: &mut EventLoop, fd: i32) -> Result<Arc<RwLock<RedisClient>>, String> {
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
        el.create_file_event(fd, Mask::Readable, Arc::new(read_query_from_client), Some(c.clone()))?;
        server_write().clients.push_back(c.clone());
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

    pub fn set_argv(&mut self, argv: Vec<Arc<RedisObject>>) {
        self.argv = argv;
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
}

pub struct WrappedClient(Arc<RwLock<RedisClient>>);

impl WrappedClient {
    pub fn inner(&self) -> Arc<RwLock<RedisClient>> {
        self.0.clone()
    }

    fn process_input_buf(&self) {
        let mut c = self.0.write().unwrap();
        // Before to process the input buffer, make sure the client is not
        // waitig for a blocking operation such as BLPOP. Note that the first
        // iteration the client is never blocked, otherwise the processInputBuffer
        // would not be called at all, but after the execution of the first commands
        // in the input buffer the client may be blocked, and the "goto again"
        // will try to reiterate. The following line will make it return asap.
        if c.flags.is_blocked() || c.flags.is_io_wait() {
            return;
        }
        if c.bulk_len == -1 {
            if c.query_buf.contains("\n") {
                // Read the first line of the query
                let query_buf_c = c.query_buf.clone();
                let mut iter = query_buf_c.lines();
                let query = iter.next().expect("first query doesn't exist");
                let remaining: Vec<&str> = iter.collect();
                c.query_buf = remaining.join("\r\n");
                if c.query_buf.ends_with("\n") {
                    c.query_buf.push_str("\r\n");
                }

                // Now we can split the query in arguments
                let argv: Vec<Arc<RedisObject>> = query.split(" ")
                    .filter(|a| !a.is_empty())
                    .map(|a| Arc::new(RedisObject::String { ptr: StringStorageType::String(a.to_string()) }))
                    .collect();
                c.argv = argv;
                if c.argv.is_empty() {
                    // Execute the command. If the client is still valid
                    // after processCommand() return and there is something
                    // on the query buffer try to process the next command.
                    if self.process_command() && !c.query_buf.is_empty() {
                        self.process_input_buf();
                    }
                } else {
                    // Nothing to process, argc == 0. Just process the query
                    // buffer if it's not empty or return to the caller
                    if !c.query_buf.is_empty() {
                        self.process_input_buf();
                    }
                }
                return;
            } else if c.query_buf.len() >= MAX_SIZE_INLINE_CMD {
                log(LogLevel::Verbose, "Client protocol error");
                // TODO: free client?
                return;
            }
        } else {
            // Bulk read handling. Note that if we are at this point
            // the client already sent a command terminated with a newline,
            // we are reading the bulk data that is actually the last
            // argument of the command.
            if c.bulk_len as usize <= c.query_buf.len() {
                let query_buf_c = c.query_buf.clone();
                let mut iter = query_buf_c.lines();
                let arg = iter.next().expect("last arg doesn't exist");
                if arg.len() != c.bulk_len as usize {
                    log(LogLevel::Warning, &format!("arg '{}' isn't consistent with bulk len '{}'", arg, c.bulk_len));
                    // TODO: free client?
                    return;
                }
                let remaining: Vec<&str> = iter.collect();
                c.query_buf = remaining.join("\r\n");
                if c.query_buf.ends_with("\n") {
                    c.query_buf.push_str("\r\n");
                }

                c.argv.push(Arc::new(RedisObject::String { ptr: StringStorageType::String(arg.to_string()) }));

                // Process the command. If the client is still valid after
                // the processing and there is more data in the buffer
                // try to parse it.
                if self.process_command() && !c.query_buf.is_empty() {
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
    fn process_command(&self) -> bool {
        // Free some memory if needed (maxmemory setting)
        {
            let mut server = server_write();
            if server.max_memory > 0 {
                server.free_memory_if_needed();
            }
        }
        
        let mut c = self.0.write().unwrap();
        // Handle the multi bulk command type. This is an alternative protocol
        // supported by Redis in order to receive commands that are composed of
        // multiple binary-safe "bulk" arguments. The latency of processing is
        // a bit higher but this allows things like multi-sets, so if this
        // protocol is used only for MSET and similar commands this is a big win.
        if c.multi_bulk == 0 && 
            c.argv.len() == 1 && 
            c.argv[0].deref().string().is_some() &&
            c.argv[0].deref().string().unwrap().string().is_some() &&
            c.argv[0].deref().string().unwrap().string().unwrap().starts_with("*") {
            
            let mbulk = c.argv[0].deref().string().unwrap().string().unwrap();
            match mbulk[1..].parse() {
                Ok(n) => { c.multi_bulk = n; },
                Err(e) => {
                    log(LogLevel::Warning, &format!("Parsing multi bulk '{}' failed: {}", mbulk, e));
                },
            }

            if c.multi_bulk <= 0 {
                c.reset();
                return true;
            } else {
                c.argv.pop();
                return true;
            }
        } else if c.multi_bulk != 0 {
            if c.bulk_len == -1 {
                let bulk = c.argv[0].deref().string().unwrap().string().unwrap();
                if bulk.starts_with("$") {
                    match bulk[1..].parse() {
                        Ok(n) => { c.bulk_len = n; },
                        Err(e) => {
                            log(LogLevel::Warning, &format!("Parsing bulk '{}' failed: {}", bulk, e));
                        },
                    }
                    c.argv.clear();
                    if c.bulk_len < 0 || c.bulk_len > 1024*1024*1024 {
                        self.add_reply_str("-ERR invalid bulk write count\r\n");
                        c.reset();
                        return true;
                    }
                    c.bulk_len += 2;    // add two bytes for CR+LF
                    return true;
                } else {
                    self.add_reply_str("-ERR multi bulk protocol error\r\n");
                    c.reset();
                    return true;
                }
            } else {
                let bulk_arg = c.argv.pop().unwrap();
                c.mbargv.push(bulk_arg);
                c.multi_bulk -= 1;
                if c.multi_bulk == 0 {
                    // Here we need to swap the multi-bulk argc/argv with the
                    // normal argc/argv of the client structure.
                    (c.argv, c.mbargv) = (c.mbargv.clone(), c.argv.clone());

                    // We need to set bulklen to something different than -1
                    // in order for the code below to process the command without
                    // to try to read the last argument of a bulk command as
                    // a special argument.
                    c.bulk_len = 0;
                    // continue below and process the command
                } else {
                    c.bulk_len = -1;
                    return true;
                }
            }
        }
        // -- end of multi bulk commands processing --

        let name_arg = c.argv[0].clone();
        let name = name_arg.string().unwrap().string().unwrap();
        // The QUIT command is handled as a special case. Normal command
        // procs are unable to close the client connection safely
        if name.eq_ignore_ascii_case("quit") {
            // TODO: free client?
            return false;
        }

        // Now lookup the command and check ASAP about trivial error conditions
        // such wrong arity, bad command name and so forth.
        let cmd = lookup_command(name);
        match cmd {
            None => {
                self.add_reply_str(&format!("-ERR unknown command '{}'\r\n", name));
                c.reset();
                return true;
            },
            Some(cmd) => {
                if (cmd.arity() > 0 && cmd.arity() != c.argv.len() as i32) ||
                    (c.argv.len() as i32) < (-cmd.arity()) {    // TODO: < 0???
                    self.add_reply_str(&format!("-ERR wrong number of arguments for '{}' command\r\n", cmd.name()));
                    c.reset();
                    return true;
                } else if server_read().max_memory > 0 && 
                    cmd.flags().is_deny_oom() &&
                    used_memory() > server_read().max_memory {
                    self.add_reply_str("-ERR command not allowed when used memory > 'maxmemory'\r\n");
                    c.reset();
                    return true;
                } else if cmd.flags().is_bulk() && c.bulk_len == -1 {
                    // This is a bulk command, we have to read the last argument yet.
                    let last_arg = c.argv.pop().unwrap();
                    let bulk = last_arg.string().unwrap().string().unwrap();
                    match bulk.parse() {
                        Ok(n) => { c.bulk_len = n; },
                        Err(e) => {
                            log(LogLevel::Warning, &format!("Parsing bulk '{}' failed: {}", bulk, e));
                        },
                    }

                    if c.bulk_len < 0 || c.bulk_len > 1024*1024*1024 {
                        self.add_reply_str("-ERR invalid bulk write count\r\n");
                        c.reset();
                        return true;
                    }
                    c.bulk_len += 2;    // add two bytes for CR+LF
                    // It is possible that the bulk read is already in the
                    // buffer. Check this condition and handle it accordingly.
                    // This is just a fast path, alternative to call processInputBuffer().
                    // It's a good idea since the code is small and this condition
                    // happens most of the times.
                    if c.query_buf.len() as i32 >= c.bulk_len {
                        let query_buf_c = c.query_buf.clone();
                        let mut iter = query_buf_c.lines();
                        let arg = iter.next().expect("bulk arg doesn't exist");
                        let remaining: Vec<&str> = iter.collect();
                        c.query_buf = remaining.join("\r\n");
                        if c.query_buf.ends_with("\n") {
                            c.query_buf.push_str("\r\n");
                        }

                        c.argv.push(Arc::new(RedisObject::String { ptr: StringStorageType::String(arg.to_string()) }));
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
                if c.flags.is_multi() && !Arc::ptr_eq(&cmd.proc(), &exec.proc()) &&
                    !Arc::ptr_eq(&cmd.proc(), &discard.proc()) {
                        // TODO
                } else {
                    // TODO: vm
                    call(self, cmd);
                }

                // Prepare the client for the next command
                c.reset();
                return true;
            },
        };
    }

    fn add_reply(&self, obj: RedisObject) {
        let mut c = self.0.write().unwrap();
        if c.reply.is_empty() &&
            (c.repl_state == ReplState::None ||
             c.repl_state == ReplState::Online) &&
            el_write().create_file_event(c.fd, Mask::Writable, 
                Arc::new(send_reply_to_client), Some(self.0.clone())).is_err() {
            return;
        }

        // TODO: vm related

        c.reply.push_back(Arc::new(obj.get_decoded()));
    }

    fn add_reply_str(&self, s: &str) {
        self.add_reply(RedisObject::String { ptr: StringStorageType::String(s.to_string()) });
    }
}

fn send_reply_to_client(el: &mut EventLoop, fd: i32, priv_data: Option<Arc<RwLock<RedisClient>>>, mask: Mask) {
    todo!()
}

fn read_query_from_client(_el: &mut EventLoop, fd: i32, priv_data: Option<Arc<RwLock<RedisClient>>>, _mask: Mask) {
    log(LogLevel::Verbose, "read_query_from_client entered");
    let priv_data_c = priv_data.clone().unwrap();
    let mut client = priv_data_c.write().unwrap();
    let mut buf = [0u8; IO_BUF_LEN];
    let mut nread = 0isize;

    unsafe {
        nread = read(fd, &mut buf[0] as *mut _ as *mut c_void, IO_BUF_LEN);
        if nread == -1 {
            if error() == EAGAIN {
                nread = 0;
            } else {
                log(LogLevel::Verbose, &format!("Reading from client: {}", *strerror(error())));
                // TODO: free client?
                return;
            }
        } else if nread == 0 {
            log(LogLevel::Verbose, "Client closed connection");
            // TODO: free client?
            return;
        }
    }
    if nread != 0 {
        match String::from_utf8(buf.to_vec()) {
            Ok(s) => {
                client.query_buf.push_str(&s);
            },
            Err(e) => {
                log(LogLevel::Warning, &format!("Parsing bytes from client failed: {}", e));
            },
        }
        client.last_interaction = timestamp().as_secs();
    } else {
        return;
    }
    if !client.flags.is_blocked() {
        log(LogLevel::Verbose, "read_query_from_client processing");
        WrappedClient(priv_data.unwrap()).process_input_buf();
    }
    log(LogLevel::Verbose, "read_query_from_client left");
}
