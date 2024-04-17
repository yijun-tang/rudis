use std::{process::exit, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}};

use once_cell::sync::Lazy;

use super::{BeforeSleepProc, EventFlag, EventLoop};



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

pub static STOP: Lazy<Box<RwLock<bool>>> = Lazy::new(|| { Box::new(RwLock::new(false)) });

pub static BEFORE_SLEEP: Lazy<Box<RwLock<Option<BeforeSleepProc>>>> = Lazy::new(|| Box::new(RwLock::new(None)));

pub fn el_read() -> RwLockReadGuard<'static, EventLoop> {
    EL.read().unwrap()
}

pub fn el_write() -> RwLockWriteGuard<'static, EventLoop> {
    EL.write().unwrap()
}

pub fn stop_read() -> RwLockReadGuard<'static, bool> {
    STOP.read().unwrap()
}

pub fn stop_write() -> RwLockWriteGuard<'static, bool> {
    STOP.write().unwrap()
}

pub fn before_sleep_r() -> RwLockReadGuard<'static, Option<BeforeSleepProc>> {
    BEFORE_SLEEP.read().unwrap()
}

pub fn before_sleep_w() -> RwLockWriteGuard<'static, Option<BeforeSleepProc>> {
    BEFORE_SLEEP.write().unwrap()
}

pub fn set_before_sleep_proc(before_sleep: Option<BeforeSleepProc>) {
    *before_sleep_w() = before_sleep;
}


pub fn ae_main() {
    *stop_write() = false;
    while !*stop_read() {
        if let Some(f) = before_sleep_r().clone() {
            f();
        }
        el_write().process_events(EventFlag::all_events());
    }
}

