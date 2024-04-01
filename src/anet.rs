//! Basic TCP socket stuff made a bit less boring.

use std::net::IpAddr;

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

pub fn nonblock(fd: i32) -> Result<(), &'static str> {
    todo!()
}

pub fn tcp_no_delay(fd: i32) -> Result<(), &'static str> {
    todo!()
}

pub fn tcp_keep_alive(fd: i32) -> Result<(), &'static str> {
    todo!()
}


