use std::{fmt::Display, fs::OpenOptions, io::{self, BufWriter, Write}, process::{abort, id}, thread::sleep, time::Duration};
use crate::util::timestamp;

use super::RedisServer;

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

impl RedisServer {
    /// TODO: more efficient buf writer and more convinent macro
    pub fn log(&self, level: LogLevel, body: &str) {
        if level.less(&self.verbosity) {
            return;
        }

        let mut writer: Option<Box<dyn Write>> = None;
        if self.log_file.is_empty() {
            writer = Some(Box::new(io::stdout()));
        } else {
            if let Ok(f) = OpenOptions::new().append(true).open(&self.log_file) {
                writer = Some(Box::new(f));
            } else {
                eprintln!("Can't open log file: {}", self.log_file);
                return;
            }
        }

        let mut buf_writer = BufWriter::new(writer.unwrap());
        let log = format!("[{}] {} {}: {}\n", id(), timestamp().as_millis(), level, body);
        match buf_writer.write_all(log.as_bytes()) {
            Ok(_) => {},
            Err(e) => { eprintln!("Can't write log: {}", e); },
        }
    }

    /// Redis generally does not try to recover from out of memory conditions
    /// when allocating objects or strings, it is not clear if it will be possible
    /// to report this condition to the client since the networking layer itself
    /// is based on heap allocation for send buffers, so we simply abort.
    /// At least the code will be simpler to read...
    pub fn oom(&self, msg: &str) {
        self.log(LogLevel::Warning, &format!("{}: Out of memory\n", msg));
        sleep(Duration::from_secs(1));
        abort();
    }
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
        let s = RedisServer::new();
        s.log(LogLevel::Notice, &format!("hello {}", "redist"));
        s.log(LogLevel::Debug, &format!("hello {}", "redist"));
    }
}
