use std::time::{Duration, SystemTime, UNIX_EPOCH};

use libc::__errno_location;

pub fn timestamp() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub fn get_time_ms() -> u128 {
    timestamp().as_millis()
}

pub fn add_ms_to_now(ms: u128) -> u128 {
    get_time_ms() + ms
}

pub fn yes_no_to_bool(s: &str) -> Result<bool, String> {
    match &s.to_ascii_lowercase()[..] {
        "yes" => { Ok(true) },
        "no" => { Ok(false) },
        _ => { Err("argument must be 'yes' or 'no'".to_string()) },
    }
}

#[cfg(target_os = "linux")]
pub fn error() -> i32 {
    unsafe {
        *__errno_location()
    }
}
