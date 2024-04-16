use std::{process::exit, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}};

use once_cell::sync::Lazy;

use super::{BeforeSleepProc, EventLoop};



pub static EL: Lazy<Arc<RwLock<EventLoop>>> = Lazy::new(|| {
    let el = match EventLoop::create() {
        Ok(el) => { Arc::new(RwLock::new(el)) },
        Err(e) => {
            eprintln!("Can't create event loop: {}", e);
            exit(1);
        },
    };
    
    el
});

pub fn el_read() -> RwLockReadGuard<'static, EventLoop> {
    EL.read().unwrap()
}

pub fn el_write() -> RwLockWriteGuard<'static, EventLoop> {
    EL.write().unwrap()
}

pub fn set_before_sleep_proc(before_sleep: Option<BeforeSleepProc>) {
    el_write().set_before_sleep_proc(before_sleep);
}

