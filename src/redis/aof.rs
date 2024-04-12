use std::{fs::OpenOptions, io::{BufRead, BufReader, Read}, process::exit};

use crate::redis::{log::LogLevel, RedisClient};

use super::RedisServer;

impl RedisServer {
    /// Replay the append log file. On error REDIS_OK is returned. On non fatal
    /// error (the append only file is zero-length) REDIS_ERR is returned. On
    /// fatal error an error message is logged and the program exists.
    pub fn load_append_only_file(&self) -> Result<(), String> {
        let mut reader: Option<Box<dyn Read>> = None;
        match OpenOptions::new().read(true).open(self.append_filename) {
            Ok(f) => {
                match f.metadata() {
                    Ok(meta_d) => {
                        if meta_d.len() == 0 {
                            self.log(LogLevel::Notice, "Empty aof file");
                            return Ok(());
                        }
                    },
                    Err(e) => {
                        self.log(LogLevel::Warning, &format!("Failed to get metadata of aof file: {}", e));
                    },
                }
                reader = Some(Box::new(f));
            }
            Err(e) => {
                self.log(LogLevel::Warning, &format!("Fatal error: can't open the append log file for reading: {}", e));
                exit(1);
            },
        }

        let read_err = |server: &RedisServer, err: &str| {
            server.log(LogLevel::Warning, &format!("Unrecoverable error reading the append only file: {err}"));
            exit(1);
        };

        let fmt_err = |server: &RedisServer| {
            server.log(LogLevel::Warning, "Bad file format reading the append only file");
            exit(1);
        };

        let mut iter = BufReader::new(reader.unwrap()).lines();
        let fake_client = Box::new(RedisClient::create_fake_client(self));
        loop {
            if let Some(line) = iter.next() {
                match line {
                    Ok(line) => {
                        if !line.starts_with("*") {
                            fmt_err(self);
                        }
                        let mut argc = 0;
                        let mut argv: Vec<String> = Vec::new();
                        if let Ok(i) = (line[1..]).parse() {
                            argc = i;
                        } else { fmt_err(self); }
                        for _ in 0..argc {
                            let mut len = 0u64;
                            if let Some(line_a) = iter.next() {
                                match line_a {
                                    Ok(line_a) => {
                                        if !line_a.starts_with("$") {
                                            fmt_err(self);
                                        }
                                        if let Ok(l) = (line_a[1..]).parse() {
                                            len = l;
                                        } else { fmt_err(self); }
                                    },
                                    Err(e) => { read_err(self, &e.to_string()); },
                                }
                            } else { fmt_err(self); }
                            if let Some(line_a) = iter.next() {
                                match line_a {
                                    Ok(line_a) => {
                                        if line_a.len() != len as usize { fmt_err(self); }
                                        argv.push(line_a);
                                    },
                                    Err(e) => { read_err(self, &e.to_string()); },
                                }
                            } else { fmt_err(self); }
                        }

                        // Command lookup
                        
                    },
                    Err(e) => {
                        read_err(self, &e.to_string());
                    },
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Cursor};


    #[test]
    fn lines_test() {
        let c = Cursor::new(String::from("a\r\r\nb\r\n"));
        let mut iter = BufReader::new(c).lines();

        assert_eq!(iter.next().unwrap().unwrap(), "a\r");
        assert_eq!(iter.next().unwrap().unwrap(), "b");
        assert!(iter.next().is_none());
    }
}
