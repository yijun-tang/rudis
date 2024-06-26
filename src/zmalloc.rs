//! zmalloc - total amount of allocated memory aware version of `malloc()`.
//! 
//! This is a wrapper allocator to count the memory usage.

use std::alloc::{System, GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

#[global_allocator]
static A: MemCounter = MemCounter;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct MemCounter;

impl MemCounter {
    pub fn used_memory() -> usize {
        ALLOCATED.load(Relaxed)
    }
}

unsafe impl GlobalAlloc for MemCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), Relaxed);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED.fetch_sub(layout.size(), Relaxed);
    }
}
