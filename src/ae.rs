//! A simple event-driven programming library. Originally I wrote this code
//! for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
//! it in form of a library for easy reuse.

use std::{any::Any, ops::{BitAnd, BitOr, Deref}, sync::{Arc, RwLock}};
use crate::util::{add_ms_to_now, get_time_ms, log, LogLevel};
use self::{el::{api_data_read, api_data_write, events_read, events_write, fired_read, max_fd_r, max_fd_w, tevent_head_r, tevent_head_w, tevent_nid_r, tevent_nid_w}, io_event::ApiState};

pub mod el;
pub mod handler;

const SET_SIZE: usize = 1024 * 10;    // Max number of fd supported

static NO_MORE: i32 = -1;

type FileProc = Arc<dyn Fn(i32, Mask) -> () + Sync + Send>;
type TimeProc = Arc<dyn Fn(u128, Option<Arc<dyn Any + Sync + Send>>) -> i32 + Sync + Send>;
type EventFinalizerProc = Arc<dyn Fn(Option<Arc<dyn Any + Sync + Send>>) -> () + Sync + Send>;
pub type BeforeSleepProc = Arc<dyn Fn() -> () + Sync + Send>;

#[derive(Clone, Copy, PartialEq)]
pub struct EventFlag(u8);

impl EventFlag {
    pub fn none() -> Self {
        EventFlag(0)
    }

    pub fn file_event() -> Self {
        EventFlag(1)
    }

    pub fn time_event() -> Self {
        EventFlag(2)
    }

    pub fn all_events() -> Self {
        EventFlag(3)
    }

    pub fn dont_wait() -> Self {
        EventFlag(4)
    }

    pub fn contains_time_event(&self) -> bool {
        (self.0 & Self::time_event().0) != 0
    }

    pub fn is_waiting(&self) -> bool {
        (self.0 & Self::dont_wait().0) == 0
    }
}

impl BitAnd for EventFlag {
    type Output = EventFlag;

    fn bitand(self, rhs: Self) -> Self::Output {
        EventFlag(self.0 & rhs.0)
    }
}

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

#[derive(Clone)]
pub struct FileEvent {
    mask: Mask,
    r_file_proc: FileProc,
    w_file_proc: FileProc,
}

pub struct TimeEvent {
    id: u128,
    when_ms: u128,
    time_proc: TimeProc,
    finalizer_proc: Option<EventFinalizerProc>,
    client_data: Option<Arc<dyn Any + Sync + Send>>,
    next: Option<Arc<RwLock<TimeEvent>>>,
}

pub struct FiredEvent {
    fd: i32,
    mask: Mask,
}

pub fn create_file_event(fd: i32, mask: Mask, proc: FileProc) -> Result<(), String> {
    log(LogLevel::Verbose, &format!("create_file_event entered {}", fd));

    if fd >= SET_SIZE as i32 {
        return Err(format!("fd should be less than {}", SET_SIZE));
    }
    api_data_read().add_event(fd, events_read()[fd as usize].mask, mask)?;
    let fe = &mut events_write()[fd as usize];
    fe.mask = fe.mask | mask;
    if mask.is_readable() {
        fe.r_file_proc = proc.clone();
    }
    if mask.is_writable() {
        fe.w_file_proc = proc;
    }
    if fd > *max_fd_r() {
        *max_fd_w() = fd;
    }

    log(LogLevel::Verbose, "create_file_event left");

    Ok(())
}
pub fn delete_file_event(fd: i32, mask: Mask) {
    log(LogLevel::Verbose, "delete_file_event entered");
    if fd >= SET_SIZE as i32 {
        return;
    }
    
    let old = events_read()[fd as usize].mask;
    if old == Mask::None {
        return;
    }
    events_write()[fd as usize].mask.disable(mask);

    if fd == *max_fd_r() && events_read()[fd as usize].mask == Mask::None {
        let mut j = *max_fd_r() - 1;
        while j >= 0 {
            if events_read()[j as usize].mask != Mask::None {
                break;
            }
            j -= 1;
        }
        *max_fd_w() = j;
    }

    match api_data_read().del_event(fd, old, mask) {
        Ok(_) => {},
        Err(err) => {
            eprintln!("{err}");
        }
    }

    log(LogLevel::Verbose, "delete_file_event left");
}


pub fn create_time_event(milliseconds: u128, proc: TimeProc, 
    client_data: Option<Arc<dyn Any + Sync + Send>>, finalizer_proc: Option<EventFinalizerProc>) -> u128 {
    let id = *tevent_nid_r();
    *tevent_nid_w() += 1;
    let te = Arc::new(RwLock::new(TimeEvent {
        id,
        when_ms: add_ms_to_now(milliseconds),
        time_proc: proc,
        finalizer_proc,
        client_data,
        next: tevent_head_w().take(),
    }));
    *tevent_head_w() = Some(te);

    id
}
pub fn delete_time_event(id: u128) -> Result<(), String> {
    let mut te = tevent_head_r().clone();
    let mut prev: Option<Arc<RwLock<TimeEvent>>> = None;
    while let Some(e) = te.clone() {
        match e.deref().read() {
            Ok(r) => {
                if r.id == id {
                    match prev {
                        Some(ref mut p) => {
                            p.deref().write().unwrap().next = e.deref().read().unwrap().next.clone();
                        },
                        None => {
                            *tevent_head_w() = e.deref().read().unwrap().next.clone();
                        },
                    }
                    if let Some(ref f) = e.deref().read().unwrap().finalizer_proc {
                        f(e.deref().write().unwrap().client_data.take());
                    }
                    return Ok(());
                }
            },
            Err(e) => { return Err(e.to_string()); },
        }
        prev = te;
        te = e.deref().read().unwrap().next.clone();
    }

    Err(format!("NO event with the specified ID ({id}) found"))
}

/// Wait for millseconds until the given file descriptor becomes
/// writable/readable/exception
#[cfg(target_os = "macos")]
pub fn wait(fd: i32, mask: Mask, milliseconds: u128) -> Result<Mask, i32> {
    use std::mem::zeroed;
    use libc::{fd_set, select, timeval, FD_ISSET, FD_SET, FD_ZERO};

    let mut timeout = timeval { tv_sec: (milliseconds / 1000) as i64, tv_usec: ((milliseconds % 1000) * 1000) as i32 };
    let mut ret_mask = Mask::None;
    let mut ret_val = 0;
    let mut rfds: fd_set;
    let mut wfds: fd_set;
    let mut efds: fd_set;

    unsafe {
        rfds = zeroed();
        wfds = zeroed();
        efds = zeroed();
        FD_ZERO(&mut rfds);
        FD_ZERO(&mut wfds);
        FD_ZERO(&mut efds);
        if mask.is_readable() {
            FD_SET(fd, &mut rfds);
        }
        if mask.is_writable() {
            FD_SET(fd, &mut wfds);
        }
        ret_val = select(fd + 1, &mut rfds, &mut wfds, &mut efds, &mut timeout);
        if ret_val > 0 {
            if FD_ISSET(fd, &mut rfds) {
                ret_mask = ret_mask | Mask::Readable;
            }
            if FD_ISSET(fd, &mut wfds) {
                ret_mask = ret_mask | Mask::Writable;
            }
            Ok(ret_mask)
        } else {
            Err(ret_val)
        }
    }
}


/// Search the first timer to fire.
/// This operation is useful to know how many time the select can be
/// put in sleep without to delay any event.
/// If there are no timers NULL is returned.
/// 
/// Note that's O(N) since time events are unsorted.
/// Possible optimizations (not needed by Redis so far, but...):
/// 1) Insert the event in order, so that the nearest is just the head.
///    Much better but still insertion or deletion of timers is O(N).
/// 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
pub fn search_nearest_timer() -> Option<Arc<RwLock<TimeEvent>>> {
    let mut te = tevent_head_r().clone();
    let mut nearest: Option<Arc<RwLock<TimeEvent>>> = None;

    while let Some(e) = te.clone() {
        if let Some(n) = nearest.clone() {
            if e.deref().read().unwrap().when_ms < n.deref().read().unwrap().when_ms {
                nearest = te;
            }
        } else {
            nearest = te;
        }
        te = e.deref().read().unwrap().next.clone();
    }

    nearest
}


/// Process every pending time event, then every pending file event
/// (that may be registered by time event callbacks just processed).
/// Without special flags the function sleeps until some file event
/// fires, or when the next time event occurrs (if any).
/// 
/// If flags is 0, the function does nothing and returns.
/// if flags has AE_ALL_EVENTS set, all the kind of events are processed.
/// if flags has AE_FILE_EVENTS set, file events are processed.
/// if flags has AE_TIME_EVENTS set, time events are processed.
/// if flags has AE_DONT_WAIT set the function returns ASAP until all
/// the events that's possible to process without to wait are processed.
/// 
/// The function returns the number of events processed.
pub fn process_events(flags: EventFlag) -> u32 {
    let mut processed = 0u32;

    // Nothing to do? return ASAP
    if (flags & EventFlag::all_events()) == EventFlag::none() {
        return processed;
    }

    // Note that we want call select() even if there are no
    // file events to process as long as we want to process time
    // events, in order to sleep until the next time event is ready
    // to fire. 
    if *max_fd_r() != -1 || (flags.contains_time_event() && flags.is_waiting()) {
        let mut shortest: Option<Arc<RwLock<TimeEvent>>> = None;
        let mut _time_val_us: Option<u128> = None;

        if flags.contains_time_event() && flags.is_waiting() {
            shortest = search_nearest_timer();
        }
        if let Some(shrtest) = shortest {
            // Calculate the time missing for the nearest
            // timer to fire.
            let now_ms = get_time_ms();
            if shrtest.deref().read().unwrap().when_ms < now_ms {
                _time_val_us = Some(0);
            } else {
                _time_val_us = Some((shrtest.deref().read().unwrap().when_ms - now_ms) * 1000);
            }
        } else {
            // If we have to check for events but need to return
            // ASAP because of AE_DONT_WAIT we need to set the timeout
            // to zero
            if !flags.is_waiting() {
                _time_val_us = Some(0);
            } else {
                // Otherwise we can block
                // wait forever
                _time_val_us = None;
            }
        }

        let num_events = api_data_write().poll(_time_val_us);
        for j in 0..num_events {
            let fd = fired_read()[j as usize].fd;
            let mask = fired_read()[j as usize].mask;
            let fe = events_read()[fd as usize].clone();
            let mut rfired = false;

            // note the fe->mask & mask & ... code: maybe an already processed
            // event removed an element that fired and we still didn't
            // processed, so we check if the event is still valid.
            if fe.mask.is_readable() && mask.is_readable() {
                rfired = true;
                let f = fe.r_file_proc.clone();
                f(fd, mask);
            }
            if fe.mask.is_writable() && mask.is_writable() {
                if !rfired || !Arc::ptr_eq(&fe.r_file_proc, &fe.w_file_proc) {
                    let f = fe.w_file_proc.clone();
                    f(fd, mask);
                }
            }
            processed += 1;
        }
    }
    // Check time events
    if flags.contains_time_event() {
        processed += process_time_events();
    }
    
    processed
}

pub fn process_time_events() -> u32 {
    let mut processed = 0u32;
    let mut te = tevent_head_r().clone();
    let max_id = *tevent_nid_r() - 1;

    while let Some(e) = te.clone() {
        // How this case happened?
        if e.deref().read().unwrap().id > max_id {
            te = e.deref().read().unwrap().next.clone();
            continue;
        }

        if e.deref().read().unwrap().when_ms <= get_time_ms() {
            let id = e.deref().read().unwrap().id;
            let client_data = e.deref().read().unwrap().client_data.clone();
            let f = e.deref().read().unwrap().time_proc.clone();
            let ret_val = f(id, client_data);
            processed += 1;
            /* After an event is processed our time event list may
            * no longer be the same, so we restart from head.
            * Still we make sure to don't process events registered
            * by event handlers itself in order to don't loop forever.
            * To do so we saved the max ID we want to handle.
            *
            * FUTURE OPTIMIZATIONS:
            * Note that this is NOT great algorithmically. Redis uses
            * a single time event so it's not a problem but the right
            * way to do this is to add the new elements on head, and
            * to flag deleted elements in a special way for later
            * deletion (putting references to the nodes to delete into
            * another linked list). */
            if ret_val != NO_MORE {
                e.deref().write().unwrap().when_ms = add_ms_to_now(ret_val as u128);
            } else {
                match delete_time_event(id) {
                    Ok(_) => {},
                    Err(err) => {
                        eprintln!("{err}");
                    },
                }
            }
            te = tevent_head_r().clone();
        } else {
            te = e.deref().read().unwrap().next.clone();
        }
    }
    processed
}

#[cfg(target_os = "linux")]
mod io_event {
    use std::mem::zeroed;
    use libc::{close, epoll_create, epoll_ctl, epoll_event, epoll_wait, strerror, EPOLLIN, EPOLLOUT, EPOLL_CTL_ADD, EPOLL_CTL_DEL, EPOLL_CTL_MOD};
    use crate::util::{error, log, LogLevel};
    use super::{el::fired_write, Mask, SET_SIZE};


    pub struct ApiState {
        epfd: i32,
        events: [epoll_event; SET_SIZE],
    }

    impl ApiState {
        pub fn add_event(&self, fd: i32, old: Mask, mut mask: Mask) -> Result<(), String> {
            log(LogLevel::Verbose, "add_event entered");
            
            let mut ee: epoll_event;
            // If the fd was already monitored for some event, we need a MOD
            // operation. Otherwise we need an ADD operation.
            let op = match old {
                Mask::None => { EPOLL_CTL_ADD },
                _ => { EPOLL_CTL_MOD },
            };

            unsafe {
                ee = zeroed();
                mask = mask | old;  // Merge old events
                if mask.is_readable() {
                    ee.events |= EPOLLIN as u32;
                }
                if mask.is_writable() {
                    ee.events |= EPOLLOUT as u32;
                }
                ee.u64 = fd as u64;
                log(LogLevel::Warning, &format!("add_event op: {}", op));
                if epoll_ctl(self.epfd, op, fd, &mut ee) == -1 {
                    log(LogLevel::Warning, &format!("add_event err: {}", *strerror(error())));
                    return Err(format!("ApiState.add_event: {}", *strerror(error())));
                }
            }
            
            Ok(())
        }

        pub fn del_event(&self, fd: i32, mut old: Mask, mask: Mask) -> Result<(), String> {
            log(LogLevel::Verbose, &format!("del_event entered {:?} - {:?}", old, mask));
            let mut ee: epoll_event;
            old.disable(mask);

            unsafe {
                ee = zeroed();
                if old.is_readable() {
                    ee.events |= EPOLLIN as u32;
                }
                if old.is_writable() {
                    ee.events |= EPOLLOUT as u32;
                }
                ee.u64 = fd as u64;    // x86 is little endian
                let ret_val = match old {
                    Mask::None => {
                        // Note, Kernel < 2.6.9 requires a non null event pointer even for
                        // EPOLL_CTL_DEL.
                        epoll_ctl(self.epfd, EPOLL_CTL_DEL, fd, &mut ee)
                    },
                    _ => {
                        epoll_ctl(self.epfd, EPOLL_CTL_MOD, fd, &mut ee)
                    },
                };
                if ret_val == -1 {
                    log(LogLevel::Warning, &format!("del_event err: {}", *strerror(error())));
                    return Err(format!("ApiState.del_event: {}", *strerror(error())));
                }
            }
            
            Ok(())
        }

        pub fn poll(&mut self, time_val_us: Option<u128>) -> i32 {
            let mut ret_val = 0;
            if let Some(tv_us) = time_val_us {
                unsafe {
                    ret_val = epoll_wait(self.epfd, &mut self.events[0], SET_SIZE as i32, (tv_us / 1000) as i32);
                }
            } else {
                unsafe {
                    ret_val = epoll_wait(self.epfd, &mut self.events[0], SET_SIZE as i32, -1);
                }
            }

            let mut num_events = 0;
            if ret_val > 0 {
                num_events = ret_val;

                for j in 0..num_events {
                    let mut mask = Mask::None;
                    let e = self.events[j as usize];

                    if (e.events & EPOLLIN as u32) != 0 {
                        mask = mask | Mask::Readable;
                    }
                    if (e.events & EPOLLOUT as u32) != 0 {
                        mask = mask | Mask::Writable;
                    }

                    fired_write()[j as usize].fd = e.u64 as i32;
                    fired_write()[j as usize].mask = mask;
                    log(LogLevel::Verbose, &format!("fd: {:?}, mask: {:?}", e.u64 as i32, mask));
                }
            }

            num_events
        }

        pub fn name() -> String {
            "epoll".to_string()
        }
    }

    impl Drop for ApiState {
        fn drop(&mut self) {
            let mut ret_no = -1;
            let mut err = String::new();
            unsafe {
                ret_no = close(self.epfd);
                err = format!("{}", *strerror(error()));
            }
            if ret_no == -1 {
                eprintln!("ApiState.drop failed: {}", err);
            }
        }
    }

    pub fn api_create() -> Result<ApiState, String> {
        let mut epfd = -1;
        let mut err = String::new();
        unsafe {
            epfd = epoll_create(1024);  // 1024 is just an hint for the kernel
            err = format!("{}", *strerror(error()));
        }
        if epfd == -1 {
            return Err(err);
        }
        Ok(ApiState { epfd, events: [epoll_event { events: 0, u64: 0  }; SET_SIZE] })
    }
}

#[cfg(target_os = "macos")]
mod io_event {
    use std::ptr::{null, null_mut};

    use libc::{close, kevent, kqueue, strerror, timespec, EVFILT_READ, EVFILT_WRITE, EV_ADD, EV_DELETE};

    use crate::util::error;

    use super::{FiredEvent, Mask, SET_SIZE};

    #[derive(Clone, Copy)]
    pub struct Kevent {
        ident: i32,
        filter: i16,
        flags: u16,
        fflags: u32,
        data: isize,
    }

    pub struct ApiState {
        kqfd: i32,
        events: [Kevent; SET_SIZE],
    }

    impl ApiState {
        pub fn add_event(&self, fd: i32, _old: Mask, mask: Mask) -> Result<(), String> {
            let mut ke = kevent {
                ident: fd as usize,
                filter: EVFILT_READ,
                flags: EV_ADD,
                fflags: 0,
                data: 0,
                udata: null_mut(),
            };
            if mask == Mask::Writable {
                ke.filter = EVFILT_WRITE;
            }
            if mask == Mask::Readable || mask == Mask::Writable {
                unsafe {
                    if kevent(self.kqfd, &ke, 1, null_mut(), 0, null()) == -1 {
                        return Err(format!("ApiState.add_event: {}", *strerror(error())));
                    }
                }
            }
            
            Ok(())
        }

        pub fn del_event(&self, fd: i32, _old: Mask, mask: Mask) -> Result<(), String> {
            let mut ke = kevent {
                ident: fd as usize,
                filter: EVFILT_READ,
                flags: EV_DELETE,
                fflags: 0,
                data: 0,
                udata: null_mut(),
            };
            if mask == Mask::Writable {
                ke.filter = EVFILT_WRITE;
            }
            if mask == Mask::Readable || mask == Mask::Writable {
                unsafe {
                    if kevent(self.kqfd, &ke, 1, null_mut(), 0, null()) == -1 {
                        return Err(format!("ApiState.del_event: {}", *strerror(error())));
                    }
                }
            }
            
            Ok(())
        }

        pub fn poll(&mut self, fired: &mut Vec<FiredEvent>, time_val_us: Option<u128>) -> i32 {
            let mut ret_val = 0;
            if let Some(tv_us) = time_val_us {
                let timeout = timespec{ tv_sec: (tv_us / 1000_000u128) as i64, tv_nsec: ((tv_us % 1000_000u128) * 1000) as i64 };
                unsafe {
                    ret_val = kevent(self.kqfd, null(), 0, &mut self.events[0] as *mut _ as *mut kevent, SET_SIZE as i32, &timeout);
                }
            } else {
                unsafe {
                    ret_val = kevent(self.kqfd, null(), 0, &mut self.events[0] as *mut _ as *mut kevent, SET_SIZE as i32, null());
                }
            }

            let mut num_events = 0;
            if ret_val > 0 {
                num_events = ret_val;

                for j in 0..num_events {
                    let mut mask = Mask::None;
                    let e = &self.events[j as usize];

                    if e.filter == EVFILT_READ {
                        mask = mask | Mask::Readable;
                    }
                    if e.filter == EVFILT_WRITE {
                        mask = mask | Mask::Writable;
                    }

                    fired[j as usize].fd = e.ident as i32;
                    fired[j as usize].mask = mask;
                }
            }

            num_events
        }

        pub fn name() -> String {
            "kqueue".to_string()
        }
    }

    impl Drop for ApiState {
        fn drop(&mut self) {
            let mut ret_no = -1;
            let mut err = String::new();
            unsafe {
                ret_no = close(self.kqfd);
                err = format!("{}", *strerror(error()));
            }
            if ret_no == -1 {
                eprintln!("ApiState.drop failed: {}", err);
            }
        }
    }

    pub fn api_create() -> Result<ApiState, String> {
        let mut kqfd = -1;
        let mut err = String::new();
        unsafe {
            kqfd = kqueue();
            err = format!("{}", *strerror(error()));
        }
        if kqfd == -1 {
            return Err(err);
        }
        Ok(ApiState { kqfd, events: [Kevent { ident: 0, filter: 0, flags: 0, fflags: 0, data: 0 }; SET_SIZE] })
    }
}

pub fn get_api_name() -> String {
    ApiState::name()
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
