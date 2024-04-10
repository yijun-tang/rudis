use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

pub fn timestamp() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}
