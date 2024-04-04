//! Basic TCP socket stuff made a bit less boring.

use std::{mem::size_of_val, net::IpAddr};

use libc::{__error, c_void, fcntl, setsockopt, strerror, F_GETFL, F_SETFL, IPPROTO_TCP, O_NONBLOCK, SOL_SOCKET, SO_KEEPALIVE, TCP_NODELAY};

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

pub fn tcp_server(port: u16, bindaddr: Vec<u8>) -> Result<(), &'static str> {
    todo!()
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


