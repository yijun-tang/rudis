use super::el::Mask;


/// 
/// I/O Multiplexing of Event Loop.
/// 

#[cfg(target_os = "linux")]
pub mod io_event {
    use std::mem::zeroed;
    use libc::{close, epoll_create, epoll_ctl, epoll_event, epoll_wait, strerror, EPOLLIN, EPOLLOUT, EPOLL_CTL_ADD, EPOLL_CTL_DEL, EPOLL_CTL_MOD};
    use crate::{ae::{el::fired_write, Mask, SET_SIZE}, util::{error, log, LogLevel}};

    pub struct ApiState {
        epfd: i32,
        events: [epoll_event; SET_SIZE],
    }

    impl ApiState {
        pub fn create() -> Result<ApiState, String> {
            let mut _epfd = -1;
            let mut _err = String::new();
            unsafe {
                _epfd = epoll_create(1024);  // 1024 is just an hint for the kernel
                _err = format!("{}", *strerror(error()));
            }
            if _epfd == -1 {
                return Err(_err);
            }
            Ok(ApiState { epfd: _epfd, events: [epoll_event { events: 0, u64: 0  }; SET_SIZE] })
        }

        pub fn add_event(&self, fd: i32, old: Mask, mut mask: Mask) -> Result<(), String> {
            // log(LogLevel::Verbose, "add_event entered");
            
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
                // log(LogLevel::Warning, &format!("add_event op: {}", op));
                if epoll_ctl(self.epfd, op, fd, &mut ee) == -1 {
                    // log(LogLevel::Warning, &format!("add_event err: {}", *strerror(error())));
                    return Err(format!("ApiState.add_event: {}", *strerror(error())));
                }
            }
            
            Ok(())
        }

        pub fn del_event(&self, fd: i32, mut old: Mask, mask: Mask) -> Result<(), String> {
            // log(LogLevel::Verbose, &format!("del_event entered {:?} - {:?}", old, mask));
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
                    // log(LogLevel::Warning, &format!("del_event err: {}", *strerror(error())));
                    return Err(format!("ApiState.del_event: {}", *strerror(error())));
                }
            }
            
            Ok(())
        }

        pub fn poll(&mut self, time_val_us: Option<u128>) -> i32 {
            let mut _ret_val = 0;
            if let Some(tv_us) = time_val_us {
                unsafe {
                    _ret_val = epoll_wait(self.epfd, &mut self.events[0], SET_SIZE as i32, (tv_us / 1000) as i32);
                }
            } else {
                unsafe {
                    _ret_val = epoll_wait(self.epfd, &mut self.events[0], SET_SIZE as i32, -1);
                }
            }

            let mut num_events = 0;
            if _ret_val > 0 {
                num_events = _ret_val;

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
                    // log(LogLevel::Verbose, &format!("fd: {:?}, mask: {:?}", e.u64 as i32, mask));
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
            let mut _ret_no = -1;
            let mut _err = String::new();
            unsafe {
                _ret_no = close(self.epfd);
                _err = format!("{}", *strerror(error()));
            }
            if _ret_no == -1 {
                eprintln!("ApiState.drop failed: {}", _err);
            }
        }
    }
}


/// Wait for millseconds until the given file descriptor becomes
/// writable/readable/exception
pub fn wait(fd: i32, mask: Mask, milliseconds: u128) -> Result<Mask, i32> {
    use std::mem::zeroed;
    use libc::{fd_set, select, timeval, FD_ISSET, FD_SET, FD_ZERO};

    let mut timeout = timeval { tv_sec: (milliseconds / 1000) as i64, tv_usec: ((milliseconds % 1000) * 1000) as i32 };
    let mut ret_mask = Mask::None;
    let mut _ret_val = 0;
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
        _ret_val = select(fd + 1, &mut rfds, &mut wfds, &mut efds, &mut timeout);
        if _ret_val > 0 {
            if FD_ISSET(fd, &mut rfds) {
                ret_mask = ret_mask | Mask::Readable;
            }
            if FD_ISSET(fd, &mut wfds) {
                ret_mask = ret_mask | Mask::Writable;
            }
            Ok(ret_mask)
        } else {
            Err(_ret_val)
        }
    }
}


#[cfg(target_os = "macos")]
pub mod io_event {
    use std::ptr::{null, null_mut};
    use libc::{close, kevent, kqueue, strerror, timespec, EVFILT_READ, EVFILT_WRITE, EV_ADD, EV_DELETE};
    use crate::{ae::{el::{fired_write, Mask}, SET_SIZE}, util::error};

    pub struct ApiState {
        kqfd: i32,
    }

    impl ApiState {
        pub fn create() -> Result<ApiState, String> {
            let mut _kqfd = -1;
            let mut _err = String::new();
            unsafe {
                _kqfd = kqueue();
                _err = format!("{}", *strerror(error()));
            }
            if _kqfd == -1 {
                return Err(_err);
            }
            Ok(ApiState { kqfd: _kqfd })
        }

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

        pub fn poll(&mut self, time_val_us: Option<u128>) -> i32 {
            let mut _ret_val = 0;
            let mut events = [kevent { ident: 0, filter: 0, flags: 0, fflags: 0, data: 0, udata: null_mut() }; SET_SIZE];
            if let Some(tv_us) = time_val_us {
                let timeout = timespec{ tv_sec: (tv_us / 1000_000u128) as i64, tv_nsec: ((tv_us % 1000_000u128) * 1000) as i64 };
                unsafe {
                    _ret_val = kevent(self.kqfd, null(), 0, &mut events[0] as *mut _ as *mut kevent, SET_SIZE as i32, &timeout);
                }
            } else {
                unsafe {
                    _ret_val = kevent(self.kqfd, null(), 0, &mut events[0] as *mut _ as *mut kevent, SET_SIZE as i32, null());
                }
            }

            let mut num_events = 0;
            if _ret_val > 0 {
                num_events = _ret_val;

                for j in 0..num_events {
                    let mut mask = Mask::None;
                    let e = &events[j as usize];

                    if e.filter == EVFILT_READ {
                        mask = mask | Mask::Readable;
                    }
                    if e.filter == EVFILT_WRITE {
                        mask = mask | Mask::Writable;
                    }

                    fired_write()[j as usize].fd = e.ident as i32;
                    fired_write()[j as usize].mask = mask;
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
            let mut _ret_no = -1;
            let mut _err = String::new();
            unsafe {
                _ret_no = close(self.kqfd);
                _err = format!("{}", *strerror(error()));
            }
            if _ret_no == -1 {
                eprintln!("ApiState.drop failed: {}", _err);
            }
        }
    }
}
