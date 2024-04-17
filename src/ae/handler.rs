use std::{any::Any, collections::LinkedList, net::Ipv4Addr, sync::{Arc, RwLock}};

use libc::{c_void, close, read, strerror, write, EAGAIN};

use crate::{anet::accept, redis::{client::{clients_read, clients_write, deleled_clients_read, RedisClient}, server_read, server_write, IO_BUF_LEN}, util::{error, log, timestamp, LogLevel}, zmalloc::used_memory};

use super::{EventLoop, Mask};

/// This function gets called every time Redis is entering the
/// main loop of the event driven library, that is, before to sleep
/// for ready file descriptors.
pub fn before_sleep() {
    if server_read().vm_enabled() && server_read().io_ready_clients().len() > 0 {
        // TODO: vm related
    }

    // Remove deleted clients
    if deleled_clients_read().len() > 0 {
        let set = deleled_clients_read();
        let mut existed: LinkedList<Arc<RwLock<RedisClient>>> = clients_read().iter()
            .filter(|c| !set.contains(&c.read().unwrap().fd()))
            .map(|c| c.clone()).collect();
        clients_write().clear();
        clients_write().append(&mut existed);
    }
}

pub fn server_cron(el: &mut EventLoop, id: u128, client_data: Option<Arc<dyn Any + Sync + Send>>) -> i32 {
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
            let size = server.dbs()[i as usize].dict().capacity();
            let used = server.dbs()[i as usize].dict().len();
            let vkeys = server.dbs()[i as usize].expires().len();
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

    1000
}

pub fn accept_handler(el: &mut EventLoop, fd: i32, mask: Mask) {
    let (c_fd, c_ip, c_port) = match accept(fd) {
        Ok((c_fd, c_ip, c_port)) => { (c_fd, c_ip, c_port) },
        Err(e) => {
            log(LogLevel::Warning, &format!("Accepting client connection: {}", e));
            return;
        },
    };
    log(LogLevel::Verbose, &format!("Accepted {}:{c_port}", Ipv4Addr::from_bits(c_ip)));
    match RedisClient::create(el, c_fd) {
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

pub fn send_reply_to_client(el: &mut EventLoop, fd: i32, mask: Mask) {
    let clients = clients_read();
    let client_r = clients.iter().filter(|e| e.read().unwrap().fd() == fd).nth(0).expect("client not found");
    let mut client = client_r.write().unwrap();

    // Use writev() if we have enough buffers to send
    // TODO:

    
    todo!()
}

pub fn read_query_from_client(_el: &mut EventLoop, fd: i32, _mask: Mask) {
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
        client.process_input_buf();
    }
}
