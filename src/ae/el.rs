use std::{any::Any, ops::BitOr, process::exit, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}};
use once_cell::sync::Lazy;
use crate::util::{log, LogLevel};
use super::{handler::proc_holder, io_event::io_event::ApiState, SET_SIZE};

/// 
/// State of Event Loop.
/// 


/// File Event
#[derive(Clone)]
pub struct FileEvent {
    pub mask: Mask,
    pub r_file_proc: FileProc,
    pub w_file_proc: FileProc,
}
/// Registered events
/// 
/// fd -> FileEvent
pub static EVENTS: Lazy<RwLock<Vec<FileEvent>>> = Lazy::new(|| {
    let mut events: Vec<FileEvent> = Vec::with_capacity(SET_SIZE);
    for _ in 0..SET_SIZE {
        events.push(FileEvent { mask: Mask::None, r_file_proc: Arc::new(proc_holder), w_file_proc: Arc::new(proc_holder) })
    }
    RwLock::new(events)
});
pub fn events_read() -> RwLockReadGuard<'static, Vec<FileEvent>> {
    EVENTS.read().unwrap()
}
pub fn events_write() -> RwLockWriteGuard<'static, Vec<FileEvent>> {
    EVENTS.write().unwrap()
}


/// Time Event
pub struct TimeEvent {
    pub id: u128,
    pub when_ms: u128,
    pub time_proc: TimeProc,
    pub finalizer_proc: Option<EventFinalizerProc>,
    pub client_data: Option<Arc<dyn Any + Sync + Send>>,
    pub next: Option<Arc<RwLock<TimeEvent>>>,
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


/// Fired Event
pub struct FiredEvent {
    pub fd: i32,
    pub mask: Mask,
}
/// fd -> FiredEvent
pub static FIRED: Lazy<RwLock<Vec<FiredEvent>>> = Lazy::new(|| {
    let mut fired: Vec<FiredEvent> = Vec::with_capacity(SET_SIZE);
    for _ in 0..SET_SIZE {
        fired.push(FiredEvent { fd: -1, mask: Mask::None });
    }
    RwLock::new(fired)
});
pub fn fired_read() -> RwLockReadGuard<'static, Vec<FiredEvent>> {
    FIRED.read().unwrap()
}
pub fn fired_write() -> RwLockWriteGuard<'static, Vec<FiredEvent>> {
    FIRED.write().unwrap()
}


/// This is used for polling API specific data
pub static API_DATA: Lazy<RwLock<ApiState>> = Lazy::new(|| {
    match ApiState::create() {
        Err(e) => {
            log(LogLevel::Warning, &e);
            exit(1);
        },
        Ok(d) => { RwLock::new(d) }
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
pub fn set_before_sleep_proc(before_sleep: Option<BeforeSleepProc>) {
    *before_sleep_w() = before_sleep;
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


pub type FileProc = Arc<dyn Fn(i32, Mask) -> () + Sync + Send>;
pub type TimeProc = Arc<dyn Fn(u128, Option<Arc<dyn Any + Sync + Send>>) -> i32 + Sync + Send>;
pub type EventFinalizerProc = Arc<dyn Fn(Option<Arc<dyn Any + Sync + Send>>) -> () + Sync + Send>;
pub type BeforeSleepProc = Arc<dyn Fn() -> () + Sync + Send>;


#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Mask {
    None,
    Readable,
    Writable,
    ReadWritable,
}
impl Mask {
    pub fn is_readable(&self) -> bool {
        *self == Self::Readable || *self == Self::ReadWritable
    }

    pub fn is_writable(&self) -> bool {
        *self == Self::Writable || *self == Self::ReadWritable
    }

    pub fn disable(&mut self, mask: Self) {
        match (*self, mask) {
            (_, Self::None) => {},
            (Self::None, _) => {},
            (_, Self::ReadWritable) => { *self = Self::None; },
            (Self::Readable, Self::Readable) => { *self = Self::None; },
            (Self::ReadWritable, Self::Readable) => { *self = Self::Writable; },
            (Self::Writable, Self::Writable) => { *self = Self::None; },
            (Self::ReadWritable, Self::Writable) => { *self = Self::Readable; },
            (_, _) => {},
        }
    }
}
impl BitOr for Mask {
    type Output = Mask;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::None, r) => r,
            (l, Self::None) => l,
            (Self::ReadWritable, _) | (_, Self::ReadWritable) => Self::ReadWritable,
            (Self::Readable, Self::Writable) | (Self::Writable, Self::Readable) => Self::ReadWritable,
            (l, _) => l,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_disable() {
        let mut mask = Mask::ReadWritable;
        mask.disable(Mask::Readable);
        assert!(mask == Mask::Writable);

        mask.disable(Mask::None);
        assert!(mask == Mask::Writable);

        mask.disable(Mask::Writable);
        assert!(mask == Mask::None);
    }
}

