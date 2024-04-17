use std::{process::exit, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}};

use once_cell::sync::Lazy;

use crate::util::{log, LogLevel};

use super::{io_event::{api_create, ApiState}, process_events, BeforeSleepProc, EventFlag, FileEvent, FiredEvent, Mask, TimeEvent, SET_SIZE};

fn TODO(fd: i32, mask: Mask) {
    todo!()
}

/// 
/// State of an event based program.
/// 

// Fired events
pub static FIRED: Lazy<Box<RwLock<Vec<FiredEvent>>>> = Lazy::new(|| {
    let mut fired: Vec<FiredEvent> = Vec::with_capacity(SET_SIZE);
    for _ in 0..SET_SIZE {
        fired.push(FiredEvent { fd: -1, mask: Mask::None });
    }
    Box::new(RwLock::new(fired))
});
pub fn fired_read() -> RwLockReadGuard<'static, Vec<FiredEvent>> {
    FIRED.read().unwrap()
}
pub fn fired_write() -> RwLockWriteGuard<'static, Vec<FiredEvent>> {
    FIRED.write().unwrap()
}


// Registered events
pub static EVENTS: Lazy<Box<RwLock<Vec<FileEvent>>>> = Lazy::new(|| {
    let mut events: Vec<FileEvent> = Vec::with_capacity(SET_SIZE);
    for _ in 0..SET_SIZE {
        events.push(FileEvent { mask: Mask::None, r_file_proc: Arc::new(TODO), w_file_proc: Arc::new(TODO) })
    }
    Box::new(RwLock::new(events))
});
pub fn events_read() -> RwLockReadGuard<'static, Vec<FileEvent>> {
    EVENTS.read().unwrap()
}
pub fn events_write() -> RwLockWriteGuard<'static, Vec<FileEvent>> {
    EVENTS.write().unwrap()
}


// This is used for polling API specific data
pub static API_DATA: Lazy<Arc<RwLock<ApiState>>> = Lazy::new(|| {
    match api_create() {
        Err(e) => {
            log(LogLevel::Warning, &e);
            exit(1);
        },
        Ok(d) => { Arc::new(RwLock::new(d)) }
    }
});
pub fn api_data_read() -> RwLockReadGuard<'static, ApiState> {
    API_DATA.read().unwrap()
}
pub fn api_data_write() -> RwLockWriteGuard<'static, ApiState> {
    API_DATA.write().unwrap()
}


pub static STOP: Lazy<Box<RwLock<bool>>> = Lazy::new(|| { Box::new(RwLock::new(false)) });
pub fn stop_read() -> RwLockReadGuard<'static, bool> {
    STOP.read().unwrap()
}
pub fn stop_write() -> RwLockWriteGuard<'static, bool> {
    STOP.write().unwrap()
}


pub static BEFORE_SLEEP: Lazy<Box<RwLock<Option<BeforeSleepProc>>>> = Lazy::new(|| Box::new(RwLock::new(None)));
pub fn before_sleep_r() -> RwLockReadGuard<'static, Option<BeforeSleepProc>> {
    BEFORE_SLEEP.read().unwrap()
}
pub fn before_sleep_w() -> RwLockWriteGuard<'static, Option<BeforeSleepProc>> {
    BEFORE_SLEEP.write().unwrap()
}


pub static MAX_FD: Lazy<RwLock<i32>> = Lazy::new(|| { RwLock::new(-1) });
pub fn max_fd_r() -> RwLockReadGuard<'static, i32> {
    MAX_FD.read().unwrap()
}
pub fn max_fd_w() -> RwLockWriteGuard<'static, i32> {
    MAX_FD.write().unwrap()
}


pub static TIME_EVENT_NEXT_ID: Lazy<Box<RwLock<u128>>> = Lazy::new(|| {Box::new(RwLock::new(0))});
pub fn tevent_nid_r() -> RwLockReadGuard<'static, u128> {
    TIME_EVENT_NEXT_ID.read().unwrap()
}
pub fn tevent_nid_w() -> RwLockWriteGuard<'static, u128> {
    TIME_EVENT_NEXT_ID.write().unwrap()
}


pub static TIME_EVENT_HEAD: Lazy<RwLock<Option<Arc<RwLock<TimeEvent>>>>> = Lazy::new(|| {
    RwLock::new(None)
});
pub fn tevent_head_r() -> RwLockReadGuard<'static, Option<Arc<RwLock<TimeEvent>>>> {
    TIME_EVENT_HEAD.read().unwrap()
}
pub fn tevent_head_w() -> RwLockWriteGuard<'static, Option<Arc<RwLock<TimeEvent>>>> {
    TIME_EVENT_HEAD.write().unwrap()
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
        process_events(EventFlag::all_events());
    }
}

