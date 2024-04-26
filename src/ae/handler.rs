use std::{any::Any, borrow::Borrow, collections::LinkedList, net::Ipv4Addr, sync::{Arc, RwLock}};
use libc::{c_void, close, read, strerror, write, EAGAIN};
use crate::{anet::accept, redis::{client::{clients_read, clients_write, deleled_clients_read, deleted_clients_write, RedisClient}, obj::{RedisObject, StringStorageType}, rdb::rdb_save_background, server_read, server_write, IO_BUF_LEN}, util::{error, log, timestamp, LogLevel}, zmalloc::used_memory};
use super::{delete_file_event, Mask};

static MAX_WRITE_PER_EVENT: usize = 1024 * 64;


/// 
/// Event Handlers of Event Loop.
/// 


/// This function gets called every time Redis is entering the
/// main loop of the event driven library, that is, before to sleep
/// for ready file descriptors.
pub fn before_sleep() {
    if server_read().vm_enabled() && server_read().io_ready_clients().len() > 0 {
        // TODO: vm related
    }

    // Remove deleted clients
    if deleled_clients_read().len() > 0 {
        {
            let set = deleled_clients_read();
            let mut existed: LinkedList<Arc<RwLock<RedisClient>>> = clients_read().iter()
                .filter(|c| !set.contains(&c.read().unwrap().fd()))
                .map(|c| c.clone()).collect();
            clients_write().clear();
            clients_write().append(&mut existed);
        }
        deleted_clients_write().clear();
    }
}


/// Time Event handler: server cron tasks
///  
pub fn server_cron(id: u128, client_data: Option<Arc<dyn Any + Sync + Send>>) -> i32 {
    let loops = server_read().cron_loops();
    server_write().set_cron_loops(loops + 1);

    // We take a cached value of the unix time in the global state because
    // with virtual memory and aging there is to store the current time
    // in objects at every object access, and accuracy is not needed.
    // To access a global var is faster than calling time(NULL)
    server_write().set_unix_time(timestamp().as_secs());

    // Show some info about non-empty databases
    {
        let server = server_read();
        for i in 0..server.dbnum() {
            let size = server.dbs()[i as usize].read().unwrap().dict.capacity();
            let used = server.dbs()[i as usize].read().unwrap().dict.len();
            let vkeys = server.dbs()[i as usize].read().unwrap().expires.len();
            if (loops % 5 == 0) && (used != 0 || vkeys != 0) {
                log(LogLevel::Verbose, &format!("DB {}: {} keys ({} volatile) in {} slots HT.", i, used, vkeys, size));
            }
        }
    }

    // We don't want to resize the hash tables while a bacground saving
    // is in progress: the saving child is created using fork() that is
    // implemented with a copy-on-write semantic in most modern systems, so
    // if we resize the HT while there is the saving child at work actually
    // a lot of memory movements in the parent will cause a lot of pages
    // copied.
    if server_read().bg_save_child_pid() == -1 {
        // Currently, we use the HashMap in std lib
    }

    // Show information about connected clients
    if loops % 5 == 0 {
        let server = server_read();
        log(LogLevel::Verbose, &format!("{} clients connected ({} slaves), {} bytes in use, {} shared objects", 
            clients_read().len() - server.slaves().len(), 
            server.slaves().len(),
            used_memory(),
            server.sharing_pool().len()));
    }

    // Close connections of timedout clients

    // Check if a background saving or AOF rewrite in progress terminated
    if server_read().bg_save_child_pid() != -1 || server_read().bg_rewrite_child_pid() != -1 {

    } else {
        // If there is not a background saving in progress check if
        // we have to save now
        let now = timestamp().as_secs();
        let dirty = server_read().dirty();
        let last_save = server_read().last_save();
        let filename = server_read().db_filename().to_string();
        for save_param in server_read().save_params() {
            if dirty >= save_param.changes() as u128 &&
                (now as i128 - last_save as i128) > save_param.seconds() as i128 {
                log(LogLevel::Warning, &format!("{} changes in {} seconds. Saving...", save_param.changes(), save_param.seconds()));
                rdb_save_background(&filename);
                break;
            }
        }
    }

    // Try to expire a few timed out keys. The algorithm used is adaptive and
    // will use few CPU cycles if there are few expiring keys, otherwise
    // it will get more aggressive to avoid that too much memory is used by
    // keys that can be removed from the keyspace.

    // Swap a few keys on disk if we are over the memory limit and VM
    // is enbled. Try to free objects from the free list first.

    // Check if we should connect to a MASTER

    1000
}


/// File Event handler: accept connection request
/// 
pub fn accept_handler(fd: i32, mask: Mask) {
    let (c_fd, c_ip, c_port) = match accept(fd) {
        Ok((c_fd, c_ip, c_port)) => { (c_fd, c_ip, c_port) },
        Err(e) => {
            log(LogLevel::Warning, &format!("Accepting client connection: {}", e));
            return;
        },
    };
    log(LogLevel::Verbose, &format!("Accepted {}:{c_port}", Ipv4Addr::from_bits(c_ip)));
    match RedisClient::create(c_fd) {
        Ok(client) => {
            // If maxclient directive is set and this is one client more... close the
            // connection. Note that we create the client instead to check before
            // for this condition, since now the socket is already set in nonblocking
            // mode and we can send an error for free using the Kernel I/O
            if server_read().max_clients() > 0 && clients_read().len() as u32 > server_read().max_clients() {
                let err = "-ERR max number of clients reached\r\n";
                unsafe {
                    // That's a best effort error message, don't check write errors
                    if write(client.read().unwrap().fd(), err as *const _ as *const c_void, err.len()) == -1 {
                    }
                }
                // TODO: free client?
                return;
            }
            let n = server_read().stat_numconnections();
            server_write().set_stat_numconnections(n + 1);
        },
        Err(e) => {
            log(LogLevel::Warning, &format!("Error allocating resoures for the client: {}", e));
            unsafe { close(c_fd); } // May be already closed, just ingore errors
            return;
        },
    }
}


/// File Event handler: send reply to client
/// 
pub fn send_reply_to_client(fd: i32, mask: Mask) {
    // log(LogLevel::Verbose, "send_reply_to_client entered");
    let clients = clients_read();
    let client_r = clients.iter().filter(|e| e.read().unwrap().fd() == fd).nth(0).expect("client not found");
    let mut client = client_r.write().unwrap();

    // Use writev() if we have enough buffers to send
    // TODO:

    let mut obj_len: usize = 0;
    let mut n_written: isize = 0;
    let mut tot_written: usize = 0;
    while client.has_reply() {
        // TODO: glue output buf

        match client.reply_front().unwrap().borrow() {
            RedisObject::String { ptr } => {
                match ptr {
                    StringStorageType::String(s) => {
                        let bytes = s.as_bytes();
                        obj_len =  bytes.len();
                        if obj_len == 0 {
                            client.reply_pop_front();
                            continue;
                        }

                        if client.flags.is_master() {
                            // Don't reply to a master
                            n_written = obj_len as isize - client.sent_len as isize;
                        } else {
                            unsafe {
                                n_written = write(client.fd(), &bytes[client.sent_len] as *const _ as *const c_void, obj_len - client.sent_len);
                            }
                            if n_written < 0 { break; }
                        }

                        client.sent_len += n_written as usize;
                        tot_written += n_written as usize;
                        // If we fully sent the object on head go to the next one
                        if client.sent_len == obj_len {
                            client.reply_pop_front();
                            client.sent_len = 0;
                        }

                        // Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
                        // bytes, in a single threaded server it's a good idea to serve
                        // other clients as well, even if a very large request comes from
                        // super fast link that is always able to accept data (in real world
                        // scenario think about 'KEYS *' against the loopback interface)
                        if tot_written > MAX_WRITE_PER_EVENT { break; }
                    },
                    StringStorageType::Integer(n) => {},
                }
            },
            _ => {},
        }
    }

    if n_written == -1 {
        if error() == EAGAIN {
            n_written = 0;
        } else {
            log(LogLevel::Verbose, &format!("Error writing to client: {}", unsafe { *strerror(error()) }));
            // TODO: free client?
            return;
        }
    }

    if tot_written > 0 {
        client.last_interaction = timestamp().as_secs();
    }
    if !client.has_reply() {
        client.sent_len = 0;
        delete_file_event(client.fd(), Mask::Writable);
    }
    // log(LogLevel::Verbose, "send_reply_to_client left");
}


/// File Event handler: read query from client
/// 
pub fn read_query_from_client(fd: i32, _mask: Mask) {
    let clients = clients_read();
    let client_r = clients.iter().filter(|e| e.read().unwrap().fd() == fd).nth(0).expect("client not found");
    let mut client = client_r.write().unwrap();
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
        let bytes = buf.to_vec().iter().take(nread as usize).map(|e| *e).collect();
        match String::from_utf8(bytes) {
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
        client.process_input_buf();
    }
}


/// File Event handler: empty handler is used for initialization
/// 
pub fn proc_holder(_fd: i32, _mask: Mask) {
}

