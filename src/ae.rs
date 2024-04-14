//! A simple event-driven programming library. Originally I wrote this code
//! for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
//! it in form of a library for easy reuse.

use std::{any::Any, mem::zeroed, ops::{BitAnd, BitOr, Deref}, ptr::{null, null_mut}, sync::{Arc, RwLock}};
use libc::{__error, close, fd_set, kevent, kqueue, select, strerror, timespec, timeval, EVFILT_READ, EVFILT_WRITE, EV_ADD, EV_DELETE, FD_ISSET, FD_SET, FD_ZERO};
use crate::util::{add_ms_to_now, get_time_ms};

const SET_SIZE: usize = 1024 * 10;    // Max number of fd supported

static NO_MORE: i32 = -1;

type FileProc = Arc<dyn Fn(&mut EventLoop, i32, Option<Arc<dyn Any + Sync + Send>>, Mask) -> () + Sync + Send>;
type TimeProc = Arc<dyn Fn(&mut EventLoop, u128, Option<Arc<dyn Any + Sync + Send>>) -> i32 + Sync + Send>;
type EventFinalizerProc = Arc<dyn Fn(&mut EventLoop, Option<Arc<dyn Any + Sync + Send>>) -> () + Sync + Send>;
pub type BeforeSleepProc = Arc<dyn Fn(&mut EventLoop) -> () + Sync + Send>;

fn TODO(el: &mut EventLoop, fd: i32, client_data: Option<Arc<dyn Any + Sync + Send>>, mask: Mask) {
    todo!()
}

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

#[derive(Clone, Copy, PartialEq)]
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

pub struct FileEvent {
    mask: Mask,
    r_file_proc: FileProc,
    w_file_proc: FileProc,
    client_data: Option<Arc<dyn Any + Sync + Send>>,
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

/// State of an event based program.
pub struct EventLoop {
    max_fd: i32,
    time_event_next_id: u128,
    events: Vec<FileEvent>,  // Registered events
    fired: Vec<FiredEvent>,  // Fired events
    time_event_head: Option<Arc<RwLock<TimeEvent>>>,
    stop: bool,
    api_data: Arc<RwLock<ApiState>>,  // This is used for polling API specific data
    before_sleep: Option<BeforeSleepProc>,
}

impl EventLoop {
    pub fn create() -> Result<EventLoop, String> {
        let api_state = Self::api_create()?;
        let mut event_loop = EventLoop {
            max_fd: -1,
            time_event_next_id: 0,
            events: Vec::with_capacity(SET_SIZE),
            fired: Vec::with_capacity(SET_SIZE),
            time_event_head: None,
            stop: false,
            api_data: Arc::new(RwLock::new(api_state)),
            before_sleep: None,
        };
        for _ in 0..SET_SIZE {
            event_loop.events.push(FileEvent { mask: Mask::None, r_file_proc: Arc::new(TODO), w_file_proc: Arc::new(TODO), client_data: None });
            event_loop.fired.push(FiredEvent { fd: -1, mask: Mask::None });
        }

        Ok(event_loop)
    }

    pub fn stop(&mut self) {
        self.stop = true;
    }

    pub fn create_file_event(&mut self, fd: i32, mask: Mask, proc: FileProc, 
        client_data: Option<Arc<dyn Any + Sync + Send>>) -> Result<(), String> {
        if fd >= SET_SIZE as i32 {
            return Err(format!("fd should be less than {}", SET_SIZE));
        }
        self.api_data.read().unwrap().add_event(fd, mask)?;
        let fe = &mut self.events[fd as usize];
        fe.mask = fe.mask | mask;
        if fe.mask.is_readable() {
            fe.r_file_proc = proc.clone();
        }
        if fe.mask.is_writable() {
            fe.w_file_proc = proc;
        }
        fe.client_data = client_data;
        if fd > self.max_fd {
            self.max_fd = fd;
        }

        Ok(())
    }

    pub fn delete_file_event(&mut self, fd: i32, mask: Mask) {
        if fd >= SET_SIZE as i32 {
            return;
        }
        let fe = &mut self.events[fd as usize];
        if fe.mask == Mask::None {
            return;
        }
        fe.mask.disable(mask);

        if fd == self.max_fd && fe.mask == Mask::None {
            let mut j = self.max_fd - 1;
            while j >= 0 {
                if self.events[j as usize].mask != Mask::None {
                    break;
                }
                j -= 1;
            }
            self.max_fd = j;
        }

        match self.api_data.read().unwrap().del_event(fd, mask) {
            Ok(_) => {},
            Err(err) => {
                eprintln!("{err}");
            }
        }
    }

    pub fn create_time_event(&mut self, milliseconds: u128, proc: TimeProc, 
        client_data: Option<Arc<dyn Any + Sync + Send>>, finalizer_proc: Option<EventFinalizerProc>) -> u128 {
        let id = self.time_event_next_id;
        self.time_event_next_id += 1;
        let te = Arc::new(RwLock::new(TimeEvent {
            id,
            when_ms: add_ms_to_now(milliseconds),
            time_proc: proc,
            finalizer_proc,
            client_data,
            next: self.time_event_head.take(),
        }));
        self.time_event_head = Some(te);

        id
    }

    pub fn delete_time_event(&mut self, id: u128) -> Result<(), String> {
        let mut te = self.time_event_head.clone();
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
                                self.time_event_head = e.deref().read().unwrap().next.clone();
                            },
                        }
                        if let Some(ref f) = e.deref().read().unwrap().finalizer_proc {
                            f(self, e.deref().write().unwrap().client_data.take());
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
    pub fn process_events(&mut self, flags: EventFlag) -> u32 {
        let mut processed = 0u32;

        // Nothing to do? return ASAP
        if (flags & EventFlag::all_events()) == EventFlag::none() {
            return processed;
        }

        // Note that we want call select() even if there are no
        // file events to process as long as we want to process time
        // events, in order to sleep until the next time event is ready
        // to fire. 
        if self.max_fd != -1 || (flags.contains_time_event() && flags.is_waiting()) {
            let mut shortest: Option<Arc<RwLock<TimeEvent>>> = None;
            let mut time_val_us: Option<u128> = None;

            if flags.contains_time_event() && flags.is_waiting() {
                shortest = self.search_nearest_timer();
            }
            if let Some(shrtest) = shortest {
                // Calculate the time missing for the nearest
                // timer to fire.
                let now_ms = get_time_ms();
                if shrtest.deref().read().unwrap().when_ms < now_ms {
                    time_val_us = Some(0);
                } else {
                    time_val_us = Some((shrtest.deref().read().unwrap().when_ms - now_ms) * 1000);
                }
            } else {
                // If we have to check for events but need to return
                // ASAP because of AE_DONT_WAIT we need to set the timeout
                // to zero
                if !flags.is_waiting() {
                    time_val_us = Some(0);
                } else {
                    // Otherwise we can block
                    // wait forever
                    time_val_us = None;
                }
            }

            let num_events = self.api_data.write().unwrap().poll(&mut self.fired, time_val_us);
            for j in 0..num_events {
                let fd = self.fired[j as usize].fd;
                let mask = self.fired[j as usize].mask;
                let fe = &self.events[fd as usize];
                let mut rfired = false;

                // note the fe->mask & mask & ... code: maybe an already processed
                // event removed an element that fired and we still didn't
                // processed, so we check if the event is still valid.
                if fe.mask.is_readable() && mask.is_readable() {
                    rfired = true;
                    let f = fe.r_file_proc.clone();
                    // f(self, fd, fe.client_data.clone(), mask);
                }
                if fe.mask.is_writable() && mask.is_writable() {
                    if !rfired || !Arc::ptr_eq(&fe.r_file_proc, &fe.w_file_proc) {
                        let f = fe.w_file_proc.clone();
                        f(self, fd, fe.client_data.clone(), mask);
                    }
                }
                processed += 1;
            }
        }
        // Check time events
        if flags.contains_time_event() {
            processed += self.process_time_events();
        }
        
        processed
    }

    /// Wait for millseconds until the given file descriptor becomes
    /// writable/readable/exception
    pub fn wait(fd: i32, mask: Mask, milliseconds: u128) -> Result<Mask, i32> {
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

    pub fn main(&mut self) {
        self.stop = false;
        while !self.stop {
            if let Some(f) = self.before_sleep.clone() {
                f(self);
            }
            self.process_events(EventFlag::all_events());
        }
    }

    pub fn get_api_name(&self) -> String {
        ApiState::name()
    }

    pub fn set_before_sleep_proc(&mut self, before_sleep: Option<BeforeSleepProc>) {
        self.before_sleep = before_sleep;
    }

    fn api_create() -> Result<ApiState, String> {
        let mut kqfd = -1;
        let mut err = String::new();
        unsafe {
            kqfd = kqueue();
            err = format!("{}", *strerror(*__error()));
        }
        if kqfd == -1 {
            return Err(err);
        }
        Ok(ApiState { kqfd, events: [Kevent { ident: 0, filter: 0, flags: 0, fflags: 0, data: 0 }; SET_SIZE] })
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
    fn search_nearest_timer(&self) -> Option<Arc<RwLock<TimeEvent>>> {
        let mut te = self.time_event_head.clone();
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

    fn process_time_events(&mut self) -> u32 {
        let mut processed = 0u32;
        let mut te = self.time_event_head.clone();
        let max_id = self.time_event_next_id - 1;

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
                let ret_val = f(self, id, client_data);
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
                    match self.delete_time_event(id) {
                        Ok(_) => {},
                        Err(err) => {
                            eprintln!("{err}");
                        },
                    }
                }
                te = self.time_event_head.clone();
            } else {
                te = e.deref().read().unwrap().next.clone();
            }
        }
        processed
    }


}

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
    fn add_event(&self, fd: i32, mask: Mask) -> Result<(), String> {
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
                    return Err(format!("ApiState.add_event: {}", *strerror(*__error())));
                }
            }
        }
        
        Ok(())
    }

    fn del_event(&self, fd: i32, mask: Mask) -> Result<(), String> {
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
                    return Err(format!("ApiState.del_event: {}", *strerror(*__error())));
                }
            }
        }
        
        Ok(())
    }

    fn poll(&mut self, fired: &mut Vec<FiredEvent>, time_val_us: Option<u128>) -> i32 {
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

    fn name() -> String {
        "kqueue".to_string()
    }
}

impl Drop for ApiState {
    fn drop(&mut self) {
        let mut ret_no = -1;
        let mut err = String::new();
        unsafe {
            ret_no = close(self.kqfd);
            err = format!("{}", *strerror(*__error()));
        }
        if ret_no == -1 {
            eprintln!("ApiState.drop failed: {}", err);
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
