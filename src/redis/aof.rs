use std::{fs::OpenOptions, io::{BufRead, BufReader, Read}, process::exit, sync::Arc};
use crate::{redis::RedisClient, util::{log, LogLevel}, zmalloc::used_memory};
use super::{cmd::lookup_command, obj::{try_object_encoding, try_object_sharing, RedisObject, StringStorageType}, RedisServer};

impl RedisServer {
    /// Replay the append log file. On error REDIS_OK is returned. On non fatal
    /// error (the append only file is zero-length) REDIS_ERR is returned. On
    /// fatal error an error message is logged and the program exists.
    pub fn load_append_only_file(&self) -> Result<(), String> {
        let mut _reader: Option<Box<dyn Read>> = None;
        match OpenOptions::new().read(true).open(&self.append_filename) {
            Ok(f) => {
                match f.metadata() {
                    Ok(meta_d) => {
                        if meta_d.len() == 0 {
                            log(LogLevel::Notice, "Empty aof file");
                            return Ok(());
                        }
                    },
                    Err(e) => {
                        log(LogLevel::Warning, &format!("Failed to get metadata of aof file: {}", e));
                    },
                }
                _reader = Some(Box::new(f));
            }
            Err(e) => {
                log(LogLevel::Warning, &format!("Fatal error: can't open the append log file for reading: {}", e));
                exit(1);
            },
        }

        let read_err = |err: &str| {
            log(LogLevel::Warning, &format!("Unrecoverable error reading the append only file: {err}"));
            exit(1);
        };

        let fmt_err = || {
            log(LogLevel::Warning, "Bad file format reading the append only file");
            exit(1);
        };

        let mut loaded_keys = 0u128;
        let mut iter = BufReader::new(_reader.unwrap()).lines();
        let mut fake_client = Box::new(RedisClient::create_fake_client());
        loop {
            if let Some(line) = iter.next() {
                match line {
                    Ok(line) => {
                        if !line.starts_with("*") {
                            fmt_err();
                        }
                        let mut argc = 0;
                        let mut argv: Vec<Arc<RedisObject>> = Vec::new();
                        if let Ok(i) = (line[1..]).parse() {
                            argc = i;
                        } else { fmt_err(); }
                        for _ in 0..argc {
                            let mut len = 0u64;
                            if let Some(line_a) = iter.next() {
                                match line_a {
                                    Ok(line_a) => {
                                        if !line_a.starts_with("$") {
                                            fmt_err();
                                        }
                                        if let Ok(l) = (line_a[1..]).parse() {
                                            len = l;
                                        } else { fmt_err(); }
                                    },
                                    Err(e) => { read_err(&e.to_string()); },
                                }
                            } else { fmt_err(); }
                            if let Some(line_a) = iter.next() {
                                match line_a {
                                    Ok(line_a) => {
                                        if line_a.len() != len as usize { fmt_err(); }
                                        argv.push(Arc::new(RedisObject::String { ptr: StringStorageType::String(line_a) }));
                                    },
                                    Err(e) => { read_err(&e.to_string()); },
                                }
                            } else { fmt_err(); }
                        }

                        // Command lookup
                        let name = argv[0].string().unwrap().string().unwrap();
                        let cmd = lookup_command(name);
                        if cmd.is_none() {
                            log(LogLevel::Warning, &format!("Unknown command '{}' reading the append only file", name));
                            exit(1);
                        }

                        // Try object sharing and encoding
                        if self.share_objects {
                            for j in 1..argc {
                                try_object_sharing(argv[j].clone());
                            }
                        }
                        if cmd.unwrap().is_bulk() {
                            try_object_encoding(argv[argc - 1].clone());
                        }

                        // Run the command in the context of a fake client
                        fake_client.set_argv(argv.clone());
                        cmd.unwrap().proc()(&mut fake_client);
                        // Discard the reply objects list from the fake client

                        // Clean up, ready for the next command


                        // Handle swapping while loading big datasets when VM is on
                        loaded_keys += 1;
                        if self.vm_enabled && (loaded_keys % 5000) == 0 {
                            while used_memory() as u128 > self.vm_max_memory {
                                if self.swap_one_object_blocking().is_err() {
                                    break;
                                }
                            }
                        }
                    },
                    Err(e) => {
                        read_err(&e.to_string());
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
