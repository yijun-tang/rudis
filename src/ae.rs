//! A simple event-driven programming library. Originally I wrote this code
//! for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
//! it in form of a library for easy reuse.

use std::{any::Any, ops::{BitAnd, Deref}, sync::{Arc, RwLock}};
use crate::util::{add_ms_to_now, get_time_ms};
use self::el::{api_data_read, api_data_write, before_sleep_r, events_read, events_write, fired_read, max_fd_r, max_fd_w, stop_read, stop_write, tevent_head_r, tevent_head_w, tevent_nid_r, tevent_nid_w, EventFinalizerProc, FileProc, Mask, TimeEvent, TimeProc};

pub mod el;
pub mod handler;
pub mod io_event;

const SET_SIZE: usize = 1024 * 10;    // Max number of fd supported
static NO_MORE: i32 = -1;

pub fn ae_main() {
    *stop_write() = false;
    while !*stop_read() {
        if let Some(f) = before_sleep_r().clone() {
            f();
        }
        process_events(EventFlag::all_events());
    }
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


pub fn create_file_event(fd: i32, mask: Mask, proc: FileProc) -> Result<(), String> {
    // log(LogLevel::Verbose, &format!("create_file_event entered {}", fd));

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

    // log(LogLevel::Verbose, "create_file_event left");

    Ok(())
}
pub fn delete_file_event(fd: i32, mask: Mask) {
    // log(LogLevel::Verbose, "delete_file_event entered");
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

    // log(LogLevel::Verbose, "delete_file_event left");
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

