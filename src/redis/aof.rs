use std::{fs::{remove_file, rename, File, OpenOptions}, io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Read, Write}, process::{exit, id}, sync::{Arc, RwLock}};
use libc::{close, fork, strerror};
use crate::{redis::{server_read, server_write, RedisClient}, util::{error, log, timestamp, LogLevel}, zmalloc::Counter};
use super::{cmd::lookup_command, obj::{try_object_encoding, try_object_sharing, RedisObject, StringStorageType}};

/// Replay the append log file. On error REDIS_OK is returned. On non fatal
/// error (the append only file is zero-length) REDIS_ERR is returned. On
/// fatal error an error message is logged and the program exists.
pub fn load_append_only_file(filename: &str) -> Result<(), String> {
    let mut _reader: Option<Box<dyn Read>> = None;
    match OpenOptions::new().read(true).open(filename) {
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
                    let mut argv: Vec<Arc<RwLock<RedisObject>>> = Vec::new();
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
                                    argv.push(Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String(line_a) })));
                                },
                                Err(e) => { read_err(&e.to_string()); },
                            }
                        } else { fmt_err(); }
                    }

                    // Command lookup
                    let arg_r = argv[0].read().unwrap();
                    let name = arg_r.string().unwrap().string().unwrap();
                    match lookup_command(name) {
                        None => {
                            log(LogLevel::Warning, &format!("Unknown command '{}' reading the append only file", name));
                            exit(1);
                        },
                        Some(cmd) => {
                            // Try object sharing and encoding
                            if server_read().share_objects {
                                for j in 1..argc {
                                    try_object_sharing(argv[j].clone());
                                }
                            }
                            if cmd.is_bulk() {
                                try_object_encoding(argv[argc - 1].clone());
                            }

                            // Run the command in the context of a fake client
                            fake_client.set_argv(argv.clone());
                            cmd.proc()(&mut fake_client);
                        },
                    }

                    
                    // Discard the reply objects list from the fake client

                    // Clean up, ready for the next command


                    // Handle swapping while loading big datasets when VM is on
                    loaded_keys += 1;
                    if server_read().vm_enabled && (loaded_keys % 5000) == 0 {
                        while Counter::used_memory() as u128 > server_read().vm_max_memory {
                            if server_read().swap_one_object_blocking().is_err() {
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

/// This is how rewriting of the append only file in background works:
/// 
/// 1) The user calls BGREWRITEAOF
/// 2) Redis calls this function, that forks():
///    2a) the child rewrite the append only file in a temp file.
///    2b) the parent accumulates differences in server.bgrewritebuf.
/// 3) When the child finished '2a' exists.
/// 4) The parent will trap the exit code, if it's OK, will append the
///    data accumulated into server.bgrewritebuf into the temp file, and
///    finally will rename(2) the temp file in the actual file name.
///    The the new file is reopened as the new append only file. Profit!
pub fn rewrite_append_only_file_background() -> bool {
    if server_read().bg_rewrite_child_pid != -1 {
        return false;
    }

    // TODO: vm related

    unsafe {
        let child_pid = fork();
        if child_pid == 0 {
            // child
            // TODO: vm related

            close(server_read().fd);
            let tmp_file = format!("temp-rewriteaof-bg-{}.aof", id());
            if rewrite_append_only_file(&tmp_file) {
                exit(0);
            } else {
                exit(1);
            }
        } else {
            // parent

            if child_pid == -1 {
                log(LogLevel::Warning, &format!("Can't rewrite append only file in background: fork: {}", *strerror(error())));
                return false;
            }
            log(LogLevel::Notice, &format!("Background append only file rewriting started by pid {}", child_pid));
            server_write().bg_rewrite_child_pid = child_pid;

            // We set appendseldb to -1 in order to force the next call to the
            // feedAppendOnlyFile() to issue a SELECT command, so the differences
            // accumulated by the parent into server.bgrewritebuf will start
            // with a SELECT statement and it will be safe to merge.
            server_write().append_sel_db = -1;
            return true;
        }
    }
}

/// Write a sequence of commands able to fully rebuild the dataset into
/// "filename". Used both by REWRITEAOF and BGREWRITEAOF.
fn rewrite_append_only_file(filename: &str) -> bool {
    // Note that we have to use a different temp name here compared to the
    // one used by rewriteAppendOnlyFileBackground() function.
    let tmp_file = format!("temp-rewriteaof-{}.aof", id());
    let mut file: Option<File> = None;
    match OpenOptions::new().create(true).write(true).open(&tmp_file) {
        Ok(f) => { file = Some(f); },
        Err(e) => {
            log(LogLevel::Warning, &format!("Failed rewriting the append only file: {}", e));
            return false;
        },
    };
    
    let w_err = |err: &str| {
        match remove_file(&tmp_file) {
            Ok(_) => {},
            Err(e) => {
                log(LogLevel::Warning, &format!("failed to delete tmp file: {}", e));
            },
        };
        log(LogLevel::Warning, &format!("Write error writing append only file on disk: {}", err));
        false
    };
    let select_cmd = "*2\r\n$6\r\nSELECT\r\n";

    {
        let mut buf_writer = BufWriter::new(file.unwrap());
        for i in 0..server_read().dbs.len() {
            if server_read().dbs[i].read().unwrap().dict.is_empty() {
                continue;
            }
            let db = server_read().dbs[i].clone();
            let db_r = db.read().unwrap();
            let mut iter = db_r.dict.iter();
            match buf_writer.write(select_cmd.as_bytes()) {
                Ok(_) => {},
                Err(e) => { return w_err(&e.to_string()); },
            }
            match write_bulk_u64(&mut buf_writer, i as u64) {
                Ok(_) => {},
                Err(e) => { return w_err(&e.to_string()); },
            }

            // Iterate this DB writing every entry
            while let Some(entry) = iter.next() {
                // If the value for this key is swapped, load a preview in memory.
                // We use a "swapped" flag to remember if we need to free the
                // value object instead to just increment the ref count anyway
                // in order to avoid copy-on-write of pages if we are forked()
                // TODO: vm related

                // Save the key and associated value
                if entry.1.read().unwrap().is_string() {
                    // Emit a SET command
                    match buf_writer.write("*3\r\n$3\r\nSET\r\n".as_bytes()) {
                        Ok(_) => {},
                        Err(e) => { return w_err(&e.to_string()); },
                    }
                    match write_bulk_raw_string(&mut buf_writer, entry.0) {
                        Ok(_) => {},
                        Err(e) => { return w_err(&e.to_string()); },
                    }
                    match write_bulk_string_object(&mut buf_writer, entry.1.clone()) {
                        Ok(_) => {},
                        Err(e) => { return w_err(&e.to_string()); },
                    }
                } else if entry.1.read().unwrap().is_list() {
                    // Emit the RPUSHes needed to rebuild the list
                    let list_r = entry.1.read().unwrap();
                    let list = list_r.list().unwrap();
                    for i in 0..list.len() {
                        match buf_writer.write("*3\r\n$5\r\nRPUSH\r\n".as_bytes()) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_raw_string(&mut buf_writer, entry.0) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_string_object(&mut buf_writer, Arc::new(RwLock::new(list.index(i as i32).unwrap()))) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                    }
                } else if entry.1.read().unwrap().is_set() {
                    // Emit the SADDs needed to rebuild the set
                    let set_r = entry.1.read().unwrap();
                    let set = set_r.set().unwrap();
                    let mut iter = set.iter();
                    while let Some(ele) = iter.next() {
                        match buf_writer.write("*3\r\n$4\r\nSADD\r\n".as_bytes()) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_raw_string(&mut buf_writer, entry.0) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_string_object(&mut buf_writer, Arc::new(RwLock::new(ele.clone()))) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                    }
                } else if entry.1.read().unwrap().is_zset() {
                    // Emit the ZADDs needed to rebuild the sorted set
                    let zset_r = entry.1.read().unwrap();
                    let zset = zset_r.zset().unwrap();
                    let mut iter = zset.dict().iter();
                    while let Some(ele) = iter.next() {
                        match buf_writer.write("*4\r\n$4\r\nZADD\r\n".as_bytes()) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_raw_string(&mut buf_writer, entry.0) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_f64(&mut buf_writer, *ele.1) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_string_object(&mut buf_writer, Arc::new(RwLock::new(ele.0.clone()))) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                    }
                } else {
                    assert!(false, "impossible code");
                }

                // Save the expire time
                match db.read().unwrap().expires.get(entry.0) {
                    Some(when) => {
                        if *when < timestamp().as_secs() {
                            continue;
                        }
                        match buf_writer.write("*3\r\n$8\r\nEXPIREAT\r\n".as_bytes()) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_raw_string(&mut buf_writer, entry.0) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match write_bulk_u64(&mut buf_writer, *when) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                    },
                    None => {},
                }

                // TODO: vm related   
            }
        }

        // Make sure data will not remain on the OS's output buffers
        match buf_writer.flush() {
            Ok(_) => {},
            Err(e) => { return w_err(&e.to_string()); },
        }
        match buf_writer.get_mut().sync_all() {
            Ok(_) => {},
            Err(e) => { return w_err(&e.to_string()); },
        }
    }   // drop the buf_writer
    
    // Use RENAME to make sure the DB file is changed atomically only
    // if the generate DB file is ok.
    match rename(&tmp_file, filename) {
        Ok(_) => {},
        Err(e) => { return w_err(&e.to_string()); },
    }
    log(LogLevel::Notice, "SYNC append only file rewrite performed");
    true
}

/// Write a double value in bulk format $<count>\r\n<payload>\r\n
fn write_bulk_f64(buf_w: &mut BufWriter<File>, val: f64) -> Result<(), Error> {
    let str = format!("{:.17}", val);
    write_bulk_raw_string(buf_w, str.as_str())
}

/// Write a long value in bulk format $<count>\r\n<payload>\r\n
fn write_bulk_u64(buf_w: &mut BufWriter<File>, val: u64) -> Result<(), Error> {
    let s = val.to_string();
    buf_w.write(format!("${}\r\n", s.len()).as_bytes())?;
    buf_w.write(format!("{}\r\n", s).as_bytes())?;
    Ok(())
}

/// Write an object into a file in the bulk format $<count>\r\n<payload>\r\n
fn write_bulk_string_object(buf_w: &mut BufWriter<File>, obj: Arc<RwLock<RedisObject>>) -> Result<(), Error> {
    match obj.read().unwrap().string() {
        Some(s_storage) => {
            match s_storage {
                StringStorageType::String(s) => write_bulk_raw_string(buf_w, s)?,
                StringStorageType::Integer(i) => write_bulk_raw_string(buf_w, i.to_string().as_str())?,
            }
        },
        None => {
            return Err(Error::new(ErrorKind::Other, "the object isn't string object"));
        },
    }
    Ok(())
}

fn write_bulk_raw_string(buf_w: &mut BufWriter<File>, str: &str) -> Result<(), Error> {
    buf_w.write(format!("${}\r\n", str.len()).as_bytes())?;
    buf_w.write(format!("{}\r\n", str).as_bytes())?;
    Ok(())
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
