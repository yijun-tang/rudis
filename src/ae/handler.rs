use std::{any::Any, net::Ipv4Addr, sync::{Arc, RwLock}};

use libc::{c_void, close, write};

use crate::{anet::accept, redis::{client::RedisClient, server_read, server_write}, util::{log, timestamp, LogLevel}, zmalloc::used_memory};

use super::{EventLoop, Mask};

/// This function gets called every time Redis is entering the
/// main loop of the event driven library, that is, before to sleep
/// for ready file descriptors.
pub fn before_sleep(_el: &mut EventLoop) {
    if server_read().vm_enabled() && server_read().io_ready_clients().len() > 0 {
        // TODO: vm related
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
            server.clients().len() - server.slaves().len(), 
            server.slaves().len(),
            used_memory(),
            server.sharing_pool().len()));
    }

    1000
}

pub fn accept_handler(el: &mut EventLoop, fd: i32, priv_data: Option<Arc<RwLock<RedisClient>>>, mask: Mask) {
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
            if server_read().max_clients() > 0 && server_read().clients().len() as u32 > server_read().max_clients() {
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