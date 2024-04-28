use std::{fmt::Display, fs::OpenOptions, io::{self, BufWriter, Write}, process::{abort, exit, id}, sync::RwLock, thread::sleep, time::{Duration, SystemTime, UNIX_EPOCH}};
use chrono::Utc;
use once_cell::sync::Lazy;

use crate::server::server_read;

/// 
/// Utility.
/// 

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

pub fn string_pattern_match(_pattern: &str, _key: &str) -> bool {
    todo!()
}

#[cfg(target_os = "linux")]
pub fn error() -> i32 {
    use libc::__errno_location;
    
    unsafe {
        *__errno_location()
    }
}

#[cfg(target_os = "macos")]
pub fn error() -> i32 {
    use libc::__error;

    unsafe {
        *__error()
    }
}

#[derive(Clone, Copy)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

impl LogLevel {
    fn less(&self, rhs: &Self) -> bool {
        match self {
            Self::Debug => {
                match rhs {
                    Self::Debug => false,
                    _ => true,
                }
            },
            Self::Verbose => {
                match rhs {
                    Self::Debug | Self::Verbose => false,
                    _ => true,
                }
            },
            Self::Notice => {
                match rhs {
                    Self::Warning => true,
                    _ => false,
                }
            },
            Self::Warning => false,
        }
    }
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ch = match self {
            Self::Debug => '.',
            Self::Verbose => '-',
            Self::Notice => '*',
            Self::Warning => '#',
        };
        write!(f, "{ch}")
    }
}

static LOG_WRITER: Lazy<RwLock<BufWriter<Box<dyn Write + Sync + Send>>>> = Lazy::new(|| {
    let server = server_read();
    let mut _writer: Option<Box<dyn Write + Sync + Send>> = None;

    if server.log_file().is_empty() {
        _writer = Some(Box::new(io::stdout()));
    } else {
        if let Ok(f) = OpenOptions::new().append(true).open(&server.log_file()) {
            _writer = Some(Box::new(f));
        } else {
            eprintln!("Can't open log file: {}", server.log_file());
            exit(1);
        }
    }

    RwLock::new(BufWriter::new(_writer.unwrap()))
});
static LOG_LEVEL: Lazy<LogLevel> = Lazy::new(|| { *server_read().verbosity() });

/// TODO: more convinent macro
pub fn log(level: LogLevel, body: &str) {
    if level.less(&LOG_LEVEL) {
        return;
    }

    let log = format!("[{}] {} {}: {}\n", id(), Utc::now().format("%e %b %Y %H:%M:%S%.3f"), level, body);
    let mut writer = LOG_WRITER.write().unwrap();
    match writer.write_all(log.as_bytes()) {
        Ok(_) => {},
        Err(e) => { eprintln!("Can't write log: {}", e); },
    }
    match writer.flush() {
        Err(e) => { eprintln!("failed to flush log: {e}"); },
        Ok(_) => {},
    }
}

/// Redis generally does not try to recover from out of memory conditions
/// when allocating objects or strings, it is not clear if it will be possible
/// to report this condition to the client since the networking layer itself
/// is based on heap allocation for send buffers, so we simply abort.
/// At least the code will be simpler to read...
pub fn oom(msg: &str) {
    log(LogLevel::Warning, &format!("{}: Out of memory\n", msg));
    sleep(Duration::from_secs(1));
    abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_test() {
        assert_eq!(format!("{}", LogLevel::Debug), ".");
        assert!(LogLevel::Debug.less(&LogLevel::Notice));
    }

    #[test]
    fn log_print_test() {
        log(LogLevel::Notice, &format!("hello {}", "redis"));
        log(LogLevel::Debug, &format!("hello {}", "redis"));
    }
}
