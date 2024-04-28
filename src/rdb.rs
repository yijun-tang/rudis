use std::{collections::{HashMap, HashSet, LinkedList}, fs::{metadata, remove_file, rename, File, OpenOptions}, io::{BufReader, BufWriter, Error, ErrorKind, Read, Write}, process::{exit, id}, str::from_utf8, sync::{Arc, RwLock}};
use libc::{close, fork, pid_t, strerror};
use lzf::{compress, decompress};
use crate::{server::{server_read, server_write, RedisDB}, util::{error, log, timestamp, LogLevel}};
use super::{obj::{try_object_encoding, ListStorageType, RedisObject, SetStorageType, StringStorageType, ZSetStorageType}, skiplist::SkipList};

// Object types only used for dumping to disk
static REDIS_EXPIRETIME: u8 = 253;
static REDIS_SELECTDB: u8 = 254;
static REDIS_EOF: u8 = 255;

// Defines related to the dump file format. To store 32 bits lengths for short
// keys requires a lot of space, so we check the most significant 2 bits of
// the first byte to interpreter the length:
// 
// 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
// 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
// 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
// 11|000000 this means: specially encoded object will follow. The six bits
//           number specify the kind of object that follows.
//           See the REDIS_RDB_ENC_* defines.
// 
// Lenghts up to 63 are stored using a single byte, most DB keys, and may
// values, will fit inside.
const REDIS_RDB_6BITLEN: u8 = 0;
const REDIS_RDB_14BITLEN: u8 = 1;
const REDIS_RDB_32BITLEN: u8 = 2;
const REDIS_RDB_ENCVAL: u8 = 3;
// When a length of a string object stored on disk has the first two bits
// set, the remaining two bits specify a special encoding for the object
// accordingly to the following defines:
const REDIS_RDB_ENC_INT8: u8 = 0;      // 8 bit signed integer
const REDIS_RDB_ENC_INT16: u8 = 1;     // 16 bit signed integer
const REDIS_RDB_ENC_INT32: u8 = 2;     // 32 bit signed integer
const REDIS_RDB_ENC_LZF: u8 = 3;       // string compressed with FASTLZ


pub fn rdb_load(filename: &str) -> bool {
    match metadata(&filename) {
        Ok(meta) => {
            if !meta.is_file() {
                log(LogLevel::Warning, &format!("specified dump file isn't a file: {}", &filename));
                return false;
            }
        },
        Err(e) => {
            log(LogLevel::Warning, &format!("dump file isn't existed: {}", e));
            return false;
        },
    }
    let mut _file: Option<File> = None;
    match OpenOptions::new().read(true).open(filename) {
        Ok(f) => { _file = Some(f); },
        Err(e) => {
            log(LogLevel::Warning, &format!("Fatal error: can't open the rdb file for reading: {}", e));
            return false;
        },
    }

    let eof_err = |err: &str| {
        log(LogLevel::Warning, err);
        log(LogLevel::Warning, "Short read or OOM loading DB. Unrecoverable error, aborting now.");
        exit(1);
    };

    let mut buf_reader = BufReader::new(_file.unwrap());
    let mut buf = [0u8; 9];
    match buf_reader.read_exact(&mut buf) {
        Ok(_) => {
            if &buf[0..5] != b"REDIS" {
                log(LogLevel::Warning, "Wrong signature trying to load DB from file");
                return false;
            }
            if &buf[5..] != b"0001" {
                log(LogLevel::Warning, &format!("Can't handle RDB format version {:?}", &buf[5..]));
                return false;
            }
        },
        Err(e) => { eof_err(&e.to_string()); },
    }

    let mut db: Option<Arc<RwLock<RedisDB>>> = None;
    loop {
        // Read type
        let mut type_ = 0u8;
        match rdb_load_type(&mut buf_reader) {
            Ok(t) => { type_ = t; },
            Err(e) => { eof_err(&e.to_string()); },
        };

        let mut expire_time = -1i128;
        if type_ == REDIS_EXPIRETIME {
            match rdb_load_time(&mut buf_reader) {
                Ok(t) => { expire_time = t as i128; },
                Err(e) => { eof_err(&e.to_string()); },
            }
            // We read the time so we need to read the object type again
            match rdb_load_type(&mut buf_reader) {
                Ok(t) => { type_ = t; },
                Err(e) => { eof_err(&e.to_string()); },
            }
        }       

        if type_ == REDIS_EOF {
            break;
        } 

        // Handle SELECT DB opcode as a special case
        if type_ == REDIS_SELECTDB {
            match rdb_load_len(&mut buf_reader) {
                Ok((db_id, _)) => {
                    if db_id >= server_read().dbnum as u64 {
                        log(LogLevel::Warning, &format!("FATAL: Data file was created with a Redis server configured to handle more than {} databases. Exiting\n", server_read().dbnum));
                        exit(1);
                    }
                    db = Some(server_read().dbs[db_id as usize].clone());
                    continue;
                },
                Err(e) => { eof_err(&e.to_string()); },
            }
        }

        // Read key
        let mut key = String::new();
        match rdb_load_raw_string(&mut buf_reader) {
            Ok(s) => { key = s; },
            Err(e) => { eof_err(&e.to_string()); },
        }

        // Read value
        let mut r_obj: Option<Arc<RwLock<RedisObject>>> = None;
        match rdb_load_object(&mut buf_reader, type_) {
            Ok(obj) => { r_obj = Some(obj); },
            Err(e) => { eof_err(&e.to_string()); },
        }

        // Add the new object in the hash table
        if db.clone().unwrap().read().unwrap().dict.contains_key(&key) {
            log(LogLevel::Warning, &format!("Loading DB, duplicated key ({}) found! Unrecoverable error, exiting now.", &key));
            exit(1);
        }
        db.clone().unwrap().write().unwrap().dict.insert(key.clone(), r_obj.unwrap().clone());

        // Set the expire time if needed
        if expire_time != -1 {
            db.clone().unwrap().write().unwrap().expires.insert(key.clone(), expire_time as u64);
            // Delete this key if already expired
            if expire_time < timestamp().as_secs() as i128 {
                db.clone().unwrap().write().unwrap().dict.remove(&key);
                db.clone().unwrap().write().unwrap().expires.remove(&key);
            }
        }
    }
    true
}

/// Load a Redis object of the specified type from the specified file.
/// On success a newly allocated object is returned, otherwise NULL.
fn rdb_load_object(buf_r: &mut BufReader<File>, type_code: u8) -> Result<Arc<RwLock<RedisObject>>, Error> {
    if type_code == 0 {
        // String
        let obj = rdb_load_string_object(buf_r)?;
        Ok(try_object_encoding(Arc::new(RwLock::new(obj))))
    } else if type_code == 1 {
        // List
        let (len, _) = rdb_load_len(buf_r)?;
        let mut list = LinkedList::new();
        for _ in 0..len {
            let s_obj = rdb_load_string_object(buf_r)?;
            list.push_back(s_obj);
        }
        Ok(Arc::new(RwLock::new(RedisObject::List { l: ListStorageType::LinkedList(list) })))
    } else if type_code == 2 {
        // Set
        let (len, _) = rdb_load_len(buf_r)?;
        let mut set = HashSet::with_capacity(len as usize);
        for _ in 0..len {
            let s_obj = rdb_load_string_object(buf_r)?;
            set.insert(s_obj);
        }
        Ok(Arc::new(RwLock::new(RedisObject::Set { s: SetStorageType::HashSet(set) })))
    } else if type_code == 3 {
        // ZSet
        let (len, _) = rdb_load_len(buf_r)?;
        let mut dict = HashMap::with_capacity(len as usize);
        let mut zsl = SkipList::new();
        for _ in 0..len {
            let s_obj = rdb_load_string_object(buf_r)?;
            let score = rdb_load_f64(buf_r)?;
            dict.insert(s_obj.clone(), score);
            zsl.insert(score, Arc::new(s_obj));
        }
        Ok(Arc::new(RwLock::new(RedisObject::ZSet { zs: ZSetStorageType::SkipList(dict, zsl) })))
    } else {
        Err(Error::new(ErrorKind::Other, "unsupported type"))
    }
}

/// For information about f64 serialization check rdb_save_f64()
fn rdb_load_f64(buf_r: &mut BufReader<File>) -> Result<f64, Error> {
    let mut buf = [0u8; 1];
    buf_r.read_exact(&mut buf)?;
    match buf[0] {
        255 => { Ok(f64::NEG_INFINITY) },
        254 => { Ok(f64::INFINITY) },
        253 => { Ok(f64::NAN) },
        _ => {
            let mut buf_v: Vec<u8> = Vec::with_capacity(buf[0] as usize);
            for _ in 0..buf[0] { buf_v.push(0); }
            buf_r.read_exact(&mut buf_v)?;
            match from_utf8(&buf_v) {
                Ok(s) => {
                    match s.parse() {
                        Ok(f) => {
                            let val: f64 = f;
                            Ok(val)
                        },
                        Err(e) => { Err(Error::new(ErrorKind::Other, e.to_string())) },
                    }
                },
                Err(e) => { Err(Error::new(ErrorKind::Other, e.to_string())) },
            }
        },
    }
}

fn rdb_load_type(buf_r: &mut BufReader<File>) -> Result<u8, Error> {
    let mut buf = [0u8; 1];
    buf_r.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn rdb_load_time(buf_r: &mut BufReader<File>) -> Result<u64, Error> {
    let mut buf = [0u8; 4];
    buf_r.read_exact(&mut buf)?;
    Ok(i32::from_ne_bytes(buf) as u64)
}

/// Load an encoded length from the DB, see the REDIS_RDB_* defines on the top
/// of this file for a description of how this are stored on disk.
/// 
/// is_encoded is set to 1 if the readed length is not actually a length but
/// an "encoding type", check the above comments for more info
fn rdb_load_len(buf_r: &mut BufReader<File>) -> Result<(u64, bool), Error> {
    let mut is_encoded = false;
    let mut buf = [0u8; 1];
    buf_r.read_exact(&mut buf)?;
    let type_ = buf[0] >> 6;
    match type_ {
        REDIS_RDB_6BITLEN => {
            return Ok(((buf[0] & 0x3F) as u64, is_encoded));
        },
        REDIS_RDB_ENCVAL => {
            is_encoded = true;
            return Ok(((buf[0] & 0x3F) as u64, is_encoded));
        },
        REDIS_RDB_14BITLEN => {
            let mut l_buf = [0u8; 1];
            buf_r.read_exact(&mut l_buf)?;
            let len = (((buf[0] & 0x3F) as u16) << 8) | (l_buf[0] as u16);
            return Ok((len as u64, is_encoded));
        },
        _ => {
            let mut l_buf = [0u8; 4];
            buf_r.read_exact(&mut l_buf)?;
            let len = u32::from_be_bytes(l_buf);
            return Ok((len as u64, is_encoded));
        },
    }
}

fn rdb_load_string_object(buf_r: &mut BufReader<File>) -> Result<RedisObject, Error> {
    let s = rdb_load_raw_string(buf_r)?;
    Ok(RedisObject::String { ptr: StringStorageType::String(s) })
}

fn rdb_load_raw_string(buf_r: &mut BufReader<File>) -> Result<String, Error> {
    let (len, is_encoded) = rdb_load_len(buf_r)?;
    if is_encoded {
        match len as u8 {
            REDIS_RDB_ENC_INT8 | REDIS_RDB_ENC_INT16 | REDIS_RDB_ENC_INT32 => {
                return rdb_load_integer(buf_r, len as u8);
            },
            REDIS_RDB_ENC_LZF => {
                return rdb_load_lzf_raw_string(buf_r);
            },
            _ => { assert!(false, "impossible code"); },
        }
    }
    
    let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
    for _ in 0..len { buf.push(0); }
    buf_r.read_exact(&mut buf)?;
    match from_utf8(&buf) {
        Ok(s) => { Ok(s.to_string()) },
        Err(e) => { Err(Error::new(ErrorKind::Other, e.to_string())) },
    }
}

fn rdb_load_integer(buf_r: &mut BufReader<File>, enc_type: u8) -> Result<String, Error> {
    let mut val = 0u32;
    match enc_type {
        REDIS_RDB_ENC_INT8 => {
            let mut buf = [0u8; 1];
            buf_r.read_exact(&mut buf)?;
            val = buf[0] as u32;
        },
        REDIS_RDB_ENC_INT16 => {
            let mut buf = [0u8; 2];
            buf_r.read_exact(&mut buf)?;
            let n = buf[0] as u16 | ((buf[1] as u16) << 8);
            val = n as u32;
        },
        REDIS_RDB_ENC_INT32 => {
            let mut buf = [0u8; 4];
            buf_r.read_exact(&mut buf)?;
            val = buf[0] as u32 | ((buf[1] as u32) << 8) | ((buf[2] as u32) << 16) | ((buf[3] as u32) << 24);
        },
        _ => { assert!(false, "impossible code"); },
    }
    Ok(val.to_string())
}

fn rdb_load_lzf_raw_string(buf_r: &mut BufReader<File>) -> Result<String, Error> {
    let (clen, _) = rdb_load_len(buf_r)?;
    let (len, _) = rdb_load_len(buf_r)?;
    let mut buf: Vec<u8> = Vec::with_capacity(clen as usize);
    for _ in 0..clen { buf.push(0); }
    buf_r.read_exact(&mut buf)?;
    match decompress(&buf, len as usize) {
        Ok(d) => {
            match from_utf8(&d) {
                Ok(s) => { Ok(s.to_string()) },
                Err(e) => { Err(Error::new(ErrorKind::Other, e.to_string())) },
            }
        },
        Err(e) => { Err(Error::new(ErrorKind::Other, e.to_string())) },
    }
}

/// Save the DB on disk. Return false on error, true on success
pub fn rdb_save(filename: &str) -> bool {
    let tmp_file = format!("temp-{}.rdb", id());
    let w_err = |err: &str| {
        match remove_file(&tmp_file) {
            Ok(_) => {},
            Err(e) => {
                log(LogLevel::Warning, &format!("failed to delete tmp file: {}", e));
            },
        };
        log(LogLevel::Warning, &format!("Write error saving DB on disk: {}", err));
        false
    };
    
    let mut _writer: Option<File> = None;
    match OpenOptions::new().create(true).write(true).open(&tmp_file) {
        Ok(file) => { _writer = Some(file); },
        Err(e) => {
            log(LogLevel::Warning, &format!("Failed saving the DB: {}", e));
            return false;
        },
    }
    {
        let mut buf_writer = BufWriter::new(_writer.unwrap());
        match buf_writer.write("REDIS0001".as_bytes()) {
            Ok(_) => {},
            Err(e) => { return w_err(&e.to_string()); },
        }
        for i in 0..server_read().dbs.len() {
            let db = server_read().dbs[i].clone();
            let dict = &db.read().unwrap().dict;
            if dict.is_empty() {
                continue;
            }

            // Write the SELECT DB opcode
            match rdb_save_type(&mut buf_writer, REDIS_SELECTDB) {
                Ok(_) => {},
                Err(e) => { return w_err(&e.to_string()); },
            }
            match rdb_save_len(&mut buf_writer, i) {
                Ok(_) => {},
                Err(e) => { return w_err(&e.to_string()); },
            }

            // Iterate this DB writing every entry
            let mut iter = dict.iter();
            while let Some(entry) = iter.next() {
                match db.read().unwrap().expires.get(entry.0) {
                    Some(when) => {
                        // Save the expire time
                        if *when < timestamp().as_secs() {
                            continue;
                        }
                        match rdb_save_type(&mut buf_writer, REDIS_EXPIRETIME) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                        match rdb_save_time(&mut buf_writer, *when) {
                            Ok(_) => {},
                            Err(e) => { return w_err(&e.to_string()); },
                        }
                    },
                    None => {},
                }

                // Save type, key, value
                match rdb_save_type(&mut buf_writer, entry.1.read().unwrap().type_code()) {
                    Ok(_) => {},
                    Err(e) => { return w_err(&e.to_string()); },
                }
                match rdb_save_raw_string(&mut buf_writer, entry.0) {
                    Ok(_) => {},
                    Err(e) => { return w_err(&e.to_string()); },
                }
                match rdb_save_object(&mut buf_writer, entry.1.clone()) {
                    Ok(_) => {},
                    Err(e) => { return w_err(&e.to_string()); },
                }
            }
        }
        // EOF opcode
        match rdb_save_type(&mut buf_writer, REDIS_EOF) {
            Ok(_) => {},
            Err(e) => { return w_err(&e.to_string()); },
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
    }

    // Use RENAME to make sure the DB file is changed atomically only
    // if the generate DB file is ok.
    match rename(&tmp_file, filename) {
        Ok(_) => {},
        Err(e) => { return w_err(&e.to_string()); },
    }
    log(LogLevel::Notice, "DB saved on disk");
    server_write().dirty += 1;
    server_write().last_save = timestamp().as_secs();
    true
}

pub fn rdb_save_background(filename: &str) -> bool {
    if server_read().bg_save_child_pid != -1 {
        return false;
    }

    unsafe {
        let child_pid: pid_t = fork();
        if child_pid == 0 {
            // child
            close(server_read().fd);
            if rdb_save(filename) {
                exit(0);
            } else {
                exit(1);
            }
        } else {
            // parent
            if child_pid == -1 {
                log(LogLevel::Warning, &format!("Can't save in background: fork: {}", *strerror(error())));
                return false;
            }
            log(LogLevel::Notice, &format!("Background saving started by pid {}", child_pid));
            server_write().bg_save_child_pid = child_pid;
            return true;
        }
    }
}

fn rdb_save_type(buf_w: &mut BufWriter<File>, type_: u8) -> Result<(), Error> {
    buf_w.write(&[type_])?;
    Ok(())
}

/// check rdbLoadLen() comments for more info
fn rdb_save_len(buf_w: &mut BufWriter<File>, len: usize) -> Result<(), Error> {
    let mut buf = [0u8; 2];
    if len < (1 << 6) {
        // Save a 6 bit len
        buf[0] = (len as u8) | (REDIS_RDB_6BITLEN << 6);
        buf_w.write(&buf[0..1])?;
    } else if len < (1 << 14) {
        // Save a 14 bit len
        buf[0] = ((len >> 8) as u8) | (REDIS_RDB_14BITLEN << 6);
        buf[1] = len as u8;
        buf_w.write(&buf)?;
    } else {
        // Save a 32 bit len
        buf[0] = REDIS_RDB_32BITLEN << 6;
        buf_w.write(&buf[0..1])?;
        let len = len as u32;
        buf_w.write(&len.to_be_bytes())?;
    }
    Ok(())
}

fn rdb_save_time(buf_w: &mut BufWriter<File>, when: u64) -> Result<(), Error> {
    let t32 = when as u32;
    buf_w.write(&t32.to_ne_bytes())?;
    Ok(())
}

/// Save a raw string as [len][data] on disk. If the object is a string
/// representation of an integer value we try to save it in a special form
fn rdb_save_raw_string(buf_w: &mut BufWriter<File>, str: &str) -> Result<(), Error> {
    // Try integer encoding
    if str.len() <= 11 {
        let mut buf = [0u8; 5];
        let enc_len = rdb_try_integer_encoding(str, &mut buf);
        if enc_len > 0 {
            buf_w.write(&buf[0..enc_len])?;
            return Ok(());
        }
    }

    // Try LZF compression - under 20 bytes it's unable to compress even
    // aaaaaaaaaaaaaaaaaa so skip it
    if server_read().rdb_compression && str.len() > 20 {
        let ret_val = rdb_save_lzf_string(buf_w, str)?;
        if ret_val > 0 {
            return Ok(());
        }
        // retval == 0 means data can't be compressed, save the old way
    }
    
    rdb_save_len(buf_w, str.len())?;
    if !str.is_empty() {
        buf_w.write(str.as_bytes())?;
    }
    Ok(())
}

/// String objects in the form "2391" "-100" without any space and with a
/// range of values that can fit in an 8, 16 or 32 bit signed value can be
/// encoded as integers to save space
fn rdb_try_integer_encoding(str: &str, buf: &mut [u8]) -> usize {
    // Check if it's possible to encode this value as a number
    let mut _value = 0i128;
    match str.parse() {
        Ok(i) => { _value = i; },
        Err(_) => { return 0; },
    };
    // If the number converted back into a string is not identical
    // then it's not possible to encode the string as integer
    if !_value.to_string().eq(str) {
        return 0;
    }

    // Finally check if it fits in our ranges
    if i8::MIN as i128 <= _value && _value <= i8::MAX as i128 {
        buf[0] = REDIS_RDB_ENCVAL << 6 | REDIS_RDB_ENC_INT8;
        buf[1] = _value as u8;
        return 2;
    } else if i16::MIN as i128 <= _value && _value <= i16::MAX as i128 {
        buf[0] = REDIS_RDB_ENCVAL << 6 | REDIS_RDB_ENC_INT16;
        buf[1] = _value as u8;
        buf[2] = (_value >> 8) as u8;
        return 3;
    } else if i32::MIN as i128 <= _value && _value <= i32::MAX as i128 {
        buf[0] = REDIS_RDB_ENCVAL << 6 | REDIS_RDB_ENC_INT32;
        buf[1] = _value as u8;
        buf[2] = (_value >> 8) as u8;
        buf[3] = (_value >> 16) as u8;
        buf[4] = (_value >> 24) as u8;
        return 5;
    }
    0
}

fn rdb_save_lzf_string(buf_w: &mut BufWriter<File>, str: &str) -> Result<usize, Error> {
    // We require at least four bytes compression for this to be worth it
    if str.len() <= 4 {
        return Ok(0);
    }
    let mut _compressed = Vec::new();
    match compress(str.as_bytes()) {
        Ok(d) => { _compressed = d; },
        Err(e) => { return Err(Error::new(ErrorKind::Other, e.to_string())) },
    }

    let byte = REDIS_RDB_ENCVAL << 6 | REDIS_RDB_ENC_LZF;
    buf_w.write(&[byte])?;
    rdb_save_len(buf_w, _compressed.len())?;
    rdb_save_len(buf_w, str.len())?;
    buf_w.write(&_compressed)?;
    Ok(_compressed.len())
}

/// Save a Redis object.
fn rdb_save_object(buf_w: &mut BufWriter<File>, obj: Arc<RwLock<RedisObject>>) -> Result<(), Error> {
    if obj.read().unwrap().is_string() {
        rdb_save_string_object(buf_w, obj.read().unwrap().string().unwrap())?;
    } else if obj.read().unwrap().is_list() {
        let obj_r = obj.read().unwrap();
        let list = obj_r.list().unwrap();
        rdb_save_len(buf_w, list.len())?;
        for i in 0..list.len() {
            rdb_save_string_object(buf_w, list.index(i as i32).unwrap().string().unwrap())?;
        }
    } else if obj.read().unwrap().is_set() {
        let obj_r = obj.read().unwrap();
        let set = obj_r.set().unwrap();
        rdb_save_len(buf_w, set.len())?;
        let mut iter = set.iter();
        while let Some(ele) = iter.next() {
            rdb_save_string_object(buf_w, ele.string().unwrap())?;
        }
    } else if obj.read().unwrap().is_zset() {
        let obj_r = obj.read().unwrap();
        let zset = obj_r.zset().unwrap();
        rdb_save_len(buf_w, zset.len())?;
        let mut iter = zset.dict().iter();
        while let Some(ele) = iter.next() {
            rdb_save_string_object(buf_w, ele.0.string().unwrap())?;
            rdb_save_f64(buf_w, *ele.1)?;
        }
    } else {
        assert!(false, "impossible code");
    }
    Ok(())
}

fn rdb_save_string_object(buf_w: &mut BufWriter<File>, s_storage: &StringStorageType) -> Result<(), Error> {
    match s_storage {
        StringStorageType::String(s) => rdb_save_raw_string(buf_w, s)?,
        StringStorageType::Integer(i) => rdb_save_raw_string(buf_w, &i.to_string())?,
    };
    Ok(())
}

/// Save a double value. Doubles are saved as strings prefixed by an unsigned
/// 8 bit integer specifing the length of the representation.
/// This 8 bit integer has special values in order to specify the following
/// conditions:
/// 253: not a number
/// 254: + inf
/// 255: - inf
fn rdb_save_f64(buf_w: &mut BufWriter<File>, val: f64) -> Result<(), Error> {
    if val.is_nan() {
        buf_w.write(&[253u8])?;
    } else if val.is_infinite() {
        if val > 0f64 {
            buf_w.write(&[254u8])?;
        } else {
            buf_w.write(&[255u8])?;
        }
    } else {
        let str = format!("{:.17}", val);
        buf_w.write(&[str.len() as u8])?;
        buf_w.write(str.as_bytes())?;
    }
    Ok(())
}

pub fn rdb_remove_temp_file(child_pid: pid_t) {
    match remove_file(&format!("temp-{}.rdb", child_pid)) {
        Ok(_) => {},
        Err(e) => {
            log(LogLevel::Warning, &format!("failed to delete tmp file: {}", e));
        },
    };
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;


    #[test]
    fn test() {
        println!("sjjfisjifjsijfisji");
        let s = format!("{:.17}", 100f64);
        let bs = s.as_bytes();
        println!("{}:{:?}",bs.len(), bs);
        match from_utf8(bs) {
            Ok(str) => {
                match str.parse() {
                    Ok(i) => {
                        let f: f64 = i;
                        println!("f: {}", f);
                    },
                    Err(_) => todo!(),
                }
            },
            Err(_) => todo!(),
        }
    }
}
