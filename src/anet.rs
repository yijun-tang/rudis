//! Basic TCP socket stuff made a bit less boring.

use std::{mem::{size_of, size_of_val, zeroed}, net::{IpAddr, Ipv4Addr}};
use libc::{__error, bind, c_void, close, fcntl, listen, setsockopt, sockaddr, sockaddr_in, socket, strerror, AF_INET, F_GETFL, F_SETFL, INADDR_ANY, IPPROTO_TCP, O_NONBLOCK, SOCK_STREAM, SOL_SOCKET, SO_KEEPALIVE, SO_REUSEADDR, TCP_NODELAY};

pub fn tcp_connect(addr: &str, port: u16) -> Result<(), &'static str> {
    todo!()
}

pub fn tcp_nonblock_connect(addr: &str, port: u16) -> Result<(), &'static str> {
    todo!()
}

pub fn read(fd: i32, buf: &mut Vec<u8>, count: i32) -> i32 {
    todo!()
}

pub fn resolve(host: &str, ip: &mut IpAddr) -> Result<(), &'static str> {
    todo!()
}

pub fn tcp_server(port: u16, bindaddr: &str) -> Result<i32, String> {
    let mut s = -1;
    let on = 1;
    let mut sa: sockaddr_in;

    unsafe {
        s = socket(AF_INET, SOCK_STREAM, 0);
        if s == -1 {
            return Err(format!("socket: {}\n", *strerror(*__error())));
        }
        if setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on as *const _ as *const c_void, size_of::<i32>() as u32) == -1 {
            close(s);
            return Err(format!("setsockopt SO_REUSEADDR: {}\n", *strerror(*__error())));
        }
        sa = zeroed();
        sa.sin_family = AF_INET as u8;
        sa.sin_port = port.to_be();     // Network byte order is big endian, or most significant byte first
        sa.sin_addr.s_addr = INADDR_ANY.to_be();
        if !bindaddr.is_empty() {
            let addr: Ipv4Addr;
            match bindaddr.parse() {
                Ok(a) => {
                    addr = a;
                    sa.sin_addr.s_addr = u32::from(addr).to_be();
                },
                Err(e) => {
                    close(s);
                    return Err(format!("Invalid bind address '{}': {}\n", bindaddr, e));
                },
            }
        }

        if bind(s, &sa as *const _ as *const sockaddr, size_of::<sockaddr>() as u32) == -1 {
            close(s);
            return Err(format!("bind: {}\n", *strerror(*__error())));
        }

        if listen(s, 511) == -1 {   // the magic 511 constant is from nginx
            close(s);
            return Err(format!("listen: {}\n", *strerror(*__error())));
        }
    }
    Ok(s)
}

pub fn accept(serversock: i32, ip: &IpAddr, port: u16) -> Result<(), &'static str> {
    todo!()
}

pub fn write(fd: i32, buf: &mut Vec<u8>, count: i32) -> i32 {
    todo!()
}

pub unsafe fn nonblock(fd: i32) -> Result<(), String> {
    // Set the socket nonblocking.
    // Note that fcntl(2) for F_GETFL and F_SETFL can't be
    // interrupted by a signal.
    // TODO: need to block signals?
    let flag = fcntl(fd, F_GETFL);
    if flag == -1 {
        return Err(format!("fcntl(F_GETFL): {}\n", *strerror(*__error())));
    }
    if fcntl(fd, F_SETFL, flag | O_NONBLOCK) == -1 {
        return Err(format!("fcntl(F_SETFL,O_NONBLOCK): {}\n", *strerror(*__error())));
    }
    Ok(())
}

pub unsafe fn tcp_no_delay(fd: i32) -> Result<(), String> {
    let yes = 1;
    if setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes as *const _ as *const c_void, size_of_val(&yes) as u32) == -1 {
        return Err(format!("setsockopt TCP_NODELAY: {}\n", *strerror(*__error())));
    }
    Ok(())
}

pub unsafe fn tcp_keep_alive(fd: i32) -> Result<(), String> {
    let yes = 1;
    if setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes as *const _ as *const c_void, size_of_val(&yes) as u32) == -1 {
        return Err(format!("setsockopt SO_KEEPALIVE: {}\n", *strerror(*__error())));
    }
    Ok(())
}


