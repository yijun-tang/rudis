//! The following is the NetBSD libc qsort implementation modified in order to
//! support partial sorting of ranges for Redis. 
//! 
//! Alternative in Rust: 
//! `core::slice::sort::quicksort`
//! 
//! 

/// Swap Type:
/// 2 -> start address align with size of `long`, element size times size of `long`
/// 0 -> element size equals to long
/// 1 -> otherwise cases
/// 
/// 
pub fn pqsort<T: PartialOrd>(a: &mut Vec<T>, n: usize, es: usize, lrange: usize, rrange: usize) {
    if a.len() < 7 {
        // Insertion Sort.
    }
}

/// The mid of three elements.
/// TODO: inline
fn med3<'a, T: PartialOrd>(a: &'a T, b: &'a T, c: &'a T) -> &'a T {
    if a < b {
        if b < c { b } else { if a < c { c } else { a } }
    } else {
        if c < b { b } else { if a < c { a } else { c } }
    }
}
