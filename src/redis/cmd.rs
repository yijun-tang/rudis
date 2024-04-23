use std::{collections::{HashMap, HashSet, LinkedList}, ops::{BitOr, Deref}, sync::{Arc, RwLock}};
use once_cell::sync::Lazy;
use crate::{redis::obj::{NULL_BULK, PONG, WRONG_TYPE_ERR}, util::{log, LogLevel}};
use super::{client::RedisClient, obj::{try_object_encoding, ListStorageType, RedisObject, SetStorageType, StringStorageType, ZSetStorageType, COLON, CRLF, C_ONE, C_ZERO, EMPTY_MULTI_BULK, NO_KEY_ERR, NULL_MULTI_BULK, OK, OUT_OF_RANGE_ERR, SYNTAX_ERR}, server_write, skiplist::SkipList};


/// 
/// Redis Commands.
/// 


pub static MAX_SIZE_INLINE_CMD: usize = 1024 * 1024 * 256;  // max bytes in inline command


/// Command Table 
static CMD_TABLE: Lazy<HashMap<&str, Arc<RedisCommand>>> = Lazy::new(|| {
    HashMap::from([
        ("ping", Arc::new(RedisCommand { name: "ping", proc: Arc::new(ping_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("exec", Arc::new(RedisCommand { name: "exec", proc: Arc::new(exec_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("discard", Arc::new(RedisCommand { name: "discard", proc: Arc::new(discard_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("auth", Arc::new(RedisCommand { name: "auth", proc: Arc::new(auth_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("exists", Arc::new(RedisCommand { name: "exists", proc: Arc::new(exists_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("del", Arc::new(RedisCommand { name: "del", proc: Arc::new(del_command), arity: -2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("type", Arc::new(RedisCommand { name: "type", proc: Arc::new(type_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("keys", Arc::new(RedisCommand { name: "keys", proc: Arc::new(keys_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("randomkey", Arc::new(RedisCommand { name: "randomkey", proc: Arc::new(randomkey_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("rename", Arc::new(RedisCommand { name: "rename", proc: Arc::new(rename_command), arity: 3, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("renamenx", Arc::new(RedisCommand { name: "renamenx", proc: Arc::new(renamenx_command), arity: 3, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("dbsize", Arc::new(RedisCommand { name: "dbsize", proc: Arc::new(dbsize_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("expire", Arc::new(RedisCommand { name: "expire", proc: Arc::new(expire_command), arity: 3, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("ttl", Arc::new(RedisCommand { name: "ttl", proc: Arc::new(ttl_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("select", Arc::new(RedisCommand { name: "select", proc: Arc::new(select_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("move", Arc::new(RedisCommand { name: "move", proc: Arc::new(move_command), arity: 3, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("flushdb", Arc::new(RedisCommand { name: "flushdb", proc: Arc::new(flushdb_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("flushall", Arc::new(RedisCommand { name: "flushall", proc: Arc::new(flushall_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),

        ("set", Arc::new(RedisCommand { name: "set", proc: Arc::new(set_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("get", Arc::new(RedisCommand { name: "get", proc: Arc::new(get_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("getset", Arc::new(RedisCommand { name: "getset", proc: Arc::new(getset_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("mget", Arc::new(RedisCommand { name: "mget", proc: Arc::new(mget_command), arity: -2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: -1, vm_keystep: 1 })),
        ("setnx", Arc::new(RedisCommand { name: "setnx", proc: Arc::new(setnx_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("mset", Arc::new(RedisCommand { name: "mset", proc: Arc::new(mset_command), arity: -3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: -1, vm_keystep: 2 })),
        ("msetnx", Arc::new(RedisCommand { name: "msetnx", proc: Arc::new(msetnx_command), arity: -3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: -1, vm_keystep: 2 })),
        ("incr", Arc::new(RedisCommand { name: "incr", proc: Arc::new(incr_command), arity: 2, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("incrby", Arc::new(RedisCommand { name: "incrby", proc: Arc::new(incrby_command), arity: 3, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("decr", Arc::new(RedisCommand { name: "decr", proc: Arc::new(decr_command), arity: 2, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("decrby", Arc::new(RedisCommand { name: "decrby", proc: Arc::new(decrby_command), arity: 3, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("rpush", Arc::new(RedisCommand { name: "rpush", proc: Arc::new(rpush_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("lpush", Arc::new(RedisCommand { name: "lpush", proc: Arc::new(lpush_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("llen", Arc::new(RedisCommand { name: "llen", proc: Arc::new(llen_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("lrange", Arc::new(RedisCommand { name: "lrange", proc: Arc::new(lrange_command), arity: 4, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("ltrim", Arc::new(RedisCommand { name: "ltrim", proc: Arc::new(ltrim_command), arity: 4, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("lindex", Arc::new(RedisCommand { name: "lindex", proc: Arc::new(lindex_command), arity: 3, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("lset", Arc::new(RedisCommand { name: "lset", proc: Arc::new(lset_command), arity: 4, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("lrem", Arc::new(RedisCommand { name: "lrem", proc: Arc::new(lrem_command), arity: 4, flags: CmdFlags::bulk(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("lpop", Arc::new(RedisCommand { name: "lpop", proc: Arc::new(lpop_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("rpop", Arc::new(RedisCommand { name: "rpop", proc: Arc::new(rpop_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("rpoplpush", Arc::new(RedisCommand { name: "rpoplpush", proc: Arc::new(rpoplpush_command), arity: 3, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 2, vm_keystep: 1 })),
        ("sadd", Arc::new(RedisCommand { name: "sadd", proc: Arc::new(sadd_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("srem", Arc::new(RedisCommand { name: "srem", proc: Arc::new(srem_command), arity: 3, flags: CmdFlags::bulk(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("spop", Arc::new(RedisCommand { name: "spop", proc: Arc::new(spop_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("smove", Arc::new(RedisCommand { name: "smove", proc: Arc::new(smove_command), arity: 4, flags: CmdFlags::bulk(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 2, vm_keystep: 1 })),
        ("scard", Arc::new(RedisCommand { name: "scard", proc: Arc::new(scard_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("sismember", Arc::new(RedisCommand { name: "sismember", proc: Arc::new(sismember_command), arity: 3, flags: CmdFlags::bulk(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("sinter", Arc::new(RedisCommand { name: "sinter", proc: Arc::new(sinter_command), arity: -2, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: -1, vm_keystep: 1 })),
        ("sinterstore", Arc::new(RedisCommand { name: "sinterstore", proc: Arc::new(sinterstore_command), arity: -3, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 2, vm_lastkey: -1, vm_keystep: 1 })),
        ("sunion", Arc::new(RedisCommand { name: "sunion", proc: Arc::new(sunion_command), arity: -2, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: -1, vm_keystep: 1 })),
        ("sunionstore", Arc::new(RedisCommand { name: "sunionstore", proc: Arc::new(sunionstore_command), arity: -3, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 2, vm_lastkey: -1, vm_keystep: 1 })),
        ("sdiff", Arc::new(RedisCommand { name: "sdiff", proc: Arc::new(sdiff_command), arity: -2, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: -1, vm_keystep: 1 })),
        ("sdiffstore", Arc::new(RedisCommand { name: "sdiffstore", proc: Arc::new(sdiffstore_command), arity: -3, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 2, vm_lastkey: -1, vm_keystep: 1 })),
        ("smembers", Arc::new(RedisCommand { name: "smembers", proc: Arc::new(sinter_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("srandmember", Arc::new(RedisCommand { name: "srandmember", proc: Arc::new(srandmember_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zadd", Arc::new(RedisCommand { name: "zadd", proc: Arc::new(zadd_command), arity: 4, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zrem", Arc::new(RedisCommand { name: "zrem", proc: Arc::new(zrem_command), arity: 3, flags: CmdFlags::bulk(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zincrby", Arc::new(RedisCommand { name: "zincrby", proc: Arc::new(zincrby_command), arity: 4, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zrange", Arc::new(RedisCommand { name: "zrange", proc: Arc::new(zrange_command), arity: -4, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zrevrange", Arc::new(RedisCommand { name: "zrevrange", proc: Arc::new(zrevrange_command), arity: -4, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zrangebyscore", Arc::new(RedisCommand { name: "zrangebyscore", proc: Arc::new(zrangebyscore_command), arity: -4, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zcard", Arc::new(RedisCommand { name: "zcard", proc: Arc::new(zcard_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zscore", Arc::new(RedisCommand { name: "zscore", proc: Arc::new(zscore_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("zremrangebyscore", Arc::new(RedisCommand { name: "zremrangebyscore", proc: Arc::new(zremrangebyscore_command), arity: 4, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("sort", Arc::new(RedisCommand { name: "sort", proc: Arc::new(sort_command), arity: -2, flags: CmdFlags::inline() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("save", Arc::new(RedisCommand { name: "save", proc: Arc::new(save_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("bgsave", Arc::new(RedisCommand { name: "bgsave", proc: Arc::new(bgsave_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("lastsave", Arc::new(RedisCommand { name: "lastsave", proc: Arc::new(lastsave_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("shutdown", Arc::new(RedisCommand { name: "shutdown", proc: Arc::new(shutdown_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("bgrewriteaof", Arc::new(RedisCommand { name: "bgrewriteaof", proc: Arc::new(bgrewriteaof_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("info", Arc::new(RedisCommand { name: "info", proc: Arc::new(info_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("monitor", Arc::new(RedisCommand { name: "monitor", proc: Arc::new(monitor_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("slaveof", Arc::new(RedisCommand { name: "slaveof", proc: Arc::new(slaveof_command), arity: 3, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
    ])
});
pub fn lookup_command(name: &str) -> Option<Arc<RedisCommand>> {
    let name = name.to_lowercase();
    CMD_TABLE.get(&name[..]).map(|e| e.clone())
}


/// Call() is the core of Redis execution of a command
/// 
pub fn call(c: &mut RedisClient, cmd: Arc<RedisCommand>) {
    let f = &cmd.proc;
    f(c);

    // log(LogLevel::Verbose, "call ing");
    // TODO

    server_write().stat_numcommands += 1;
}


pub struct RedisCommand {
    name: &'static str,
    proc: CommandProc,
    arity: i32,
    flags: CmdFlags,
    // Use a function to determine which keys need to be loaded
    // in the background prior to executing this command. Takes precedence
    // over vm_firstkey and others, ignored when NULL
    vm_preload_proc: Option<CommandProc>,
    // What keys should be loaded in background when calling this command?
    vm_firstkey: i32,           // The first argument that's a key (0 = no keys)
    vm_lastkey: i32,            // The last argument that's a key
    vm_keystep: i32,            // The step between first and last key
}
impl RedisCommand {
    pub fn arity(&self) -> i32 {
        self.arity
    }
    pub fn name(&self) -> &str {
        self.name
    }
    pub fn flags(&self) -> &CmdFlags {
        &self.flags
    }
    pub fn is_bulk(&self) -> bool {
        self.flags.is_bulk()
    }
    pub fn proc(&self) -> CommandProc {
        self.proc.clone()
    }
}


/// Client MULTI/EXEC state
pub struct MultiCmd {
    argv: Vec<Arc<RedisObject>>,
    cmd: RedisCommand,
}

type CommandProc = Arc<dyn Fn(&mut RedisClient) -> () + Sync + Send>;

/// Command flags
pub struct CmdFlags(u8);
impl CmdFlags {
    /// Bulk write command
    fn bulk() -> CmdFlags {
        CmdFlags(1)
    }
    /// Inline command
    fn inline() -> CmdFlags {
        CmdFlags(2)
    }
    /// REDIS_CMD_DENYOOM reserves a longer comment: all the commands marked with
    /// this flags will return an error when the 'maxmemory' option is set in the
    /// config file and the server is using more than maxmemory bytes of memory.
    /// In short this commands are denied on low memory conditions.
    fn deny_oom() -> CmdFlags {
        CmdFlags(4)
    }
    pub fn is_bulk(&self) -> bool {
        (self.0 & Self::bulk().0) != 0
    }
    pub fn is_deny_oom(&self) -> bool {
        (self.0 & Self::deny_oom().0) != 0
    }
}
impl BitOr for CmdFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        CmdFlags(self.0 | rhs.0)
    }
}


fn ping_command(c: &mut RedisClient) {
    c.add_reply(PONG.clone());
}
pub fn exec_command(c: &mut RedisClient) {
    todo!()
}
pub fn discard_command(c: &mut RedisClient) {
    todo!()
}

fn auth_command(c: &mut RedisClient) {

}

fn exists_command(c: &mut RedisClient) {
    
}

fn del_command(c: &mut RedisClient) {
    
}

fn type_command(c: &mut RedisClient) {
    
}

fn keys_command(c: &mut RedisClient) {
    
}

fn randomkey_command(c: &mut RedisClient) {
    
}

fn rename_command(c: &mut RedisClient) {
    
}

fn renamenx_command(c: &mut RedisClient) {
    
}

fn dbsize_command(c: &mut RedisClient) {
    
}

fn expire_command(c: &mut RedisClient) {
    
}


fn ttl_command(c: &mut RedisClient) {
    
}

fn select_command(c: &mut RedisClient) {
    
}

fn move_command(c: &mut RedisClient) {
    
}

fn flushdb_command(c: &mut RedisClient) {
    
}

fn flushall_command(c: &mut RedisClient) {
    
}

// 
// string
// 

fn get_command(c: &mut RedisClient) {
    match get_generic_command(c) {
        Ok(_) => {},
        Err(e) => {
            log(LogLevel::Warning, &e);
        },
    }
}
fn get_generic_command(c: &RedisClient) -> Result<(), String> {
    match c.lookup_key_read_or_reply(c.argv[1].read().unwrap().as_key(), NULL_BULK.clone()) {
        None => Ok(()),
        Some(obj) => {
            if obj.read().unwrap().is_string() {
                c.add_reply_bulk(obj);
                Ok(())
            } else {
                c.add_reply(WRONG_TYPE_ERR.clone());
                Err("WRONG TYPE ERROR".to_string())
            }
        },
    }
}

fn set_command(c: &mut RedisClient) {
    set_generic_command(c, false);
}
fn set_generic_command(c: &mut RedisClient, nx: bool) {
    if nx {
        c.delete_if_volatile(c.argv[1].read().unwrap().as_key());
    }

    if c.contains(c.argv[1].read().unwrap().as_key()) {
        if nx {
            c.add_reply(C_ZERO.clone());
            return;
        } else {
            // If the key is about a swapped value, we want a new key object
            // to overwrite the old. So we delete the old key in the database.
            // This will also make sure that swap pages about the old object
            // will be marked as free.
            // TODO: vm related   
        }
    }
    c.insert(c.argv[1].read().unwrap().as_key(), c.argv[2].clone());

    server_write().dirty += 1;
    c.remove_expire(c.argv[1].read().unwrap().as_key());
    match nx {
        true => { c.add_reply(C_ONE.clone()); }
        false => { c.add_reply(OK.clone()); }
    }
}

fn getset_command(c: &mut RedisClient) {
    match get_generic_command(c) {
        Ok(_) => {},
        Err(e) => {
            log(LogLevel::Warning, &e);
            return;
        }
    }

    c.insert(c.argv[1].read().unwrap().as_key(), c.argv[2].clone());
    server_write().dirty += 1;
    c.remove_expire(c.argv[1].read().unwrap().as_key());
}

fn mget_command(c: &mut RedisClient) {
    c.add_reply_str(&format!("*{}\r\n", c.argv.len() - 1));
    for i in 1..c.argv.len() {
        match c.lookup_key_read(c.argv[i].read().unwrap().as_key()) {
            None => { c.add_reply(NULL_BULK.clone()); },
            Some(v) => {
                if v.read().unwrap().is_string() {
                    c.add_reply_bulk(v);
                } else {
                    c.add_reply(NULL_BULK.clone());
                }
            },
        }
    }
}

fn setnx_command(c: &mut RedisClient) {
    set_generic_command(c, true);
}

fn mset_command(c: &mut RedisClient) {
    mset_generic_command(c, false);
}

fn mset_generic_command(c: &mut RedisClient, nx: bool) {
    if c.argv.len() % 2 == 0 {
        c.add_reply_str("-ERR wrong number of arguments for MSET\r\n");
        return;
    }

    // Handle the NX flag. The MSETNX semantic is to return zero and don't
    // set nothing at all if at least one already key exists.
    let mut busy_keys = 0;
    if nx {
        for i in (1..c.argv.len()).step_by(2) {
            if c.lookup_key_write(c.argv[i].read().unwrap().as_key()).is_some() {
                busy_keys += 1;
            }
        }
    }
    if busy_keys > 0 {
        c.add_reply(C_ZERO.clone());
        return;
    }

    for i in (1..c.argv.len()).step_by(2) {
        c.argv[i + 1] = try_object_encoding(c.argv[i + 1].clone());
        c.insert(c.argv[i].read().unwrap().as_key(), c.argv[i + 1].clone());
        c.remove_expire(c.argv[i].read().unwrap().as_key());
    }
    server_write().dirty += (c.argv.len() as u128 - 1) / 2;
    match nx {
        true => { c.add_reply(C_ONE.clone()); }
        false => { c.add_reply(OK.clone()); }
    }
}

fn msetnx_command(c: &mut RedisClient) {
    mset_generic_command(c, true);
}

fn incr_command(c: &mut RedisClient) {
    incr_decr_command(c, 1);
}

fn incrby_command(c: &mut RedisClient) {
    let mut _i = 0i128;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(v) => { _i = v; },
        Err(e) => {
            log(LogLevel::Warning, &e.to_string());
            return;
        },
    }
    incr_decr_command(c, _i);
}

fn decr_command(c: &mut RedisClient) {
    incr_decr_command(c, -1);
}

fn decrby_command(c: &mut RedisClient) {
    let mut _i = 0i128;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(v) => { _i = v; },
        Err(e) => {
            log(LogLevel::Warning, &e.to_string());
            return;
        },
    }
    incr_decr_command(c, -_i);
}

fn incr_decr_command(c: &mut RedisClient, incr: i128) {
    let mut value = 0i128;
    match c.lookup_key_write(c.argv[1].read().unwrap().as_key()) {
        None => {},
        Some(v) => {
            match v.read().unwrap().string() {
                Some(str_storage) => {
                    match str_storage {
                        StringStorageType::String(s) => {
                            match s.parse() {
                                Ok(v) => { value = v; },
                                Err(e) => {
                                    log(LogLevel::Warning, &e.to_string());
                                    return;
                                },
                            }
                        },
                        StringStorageType::Integer(n) => { value = *n as i128; },
                    }
                },
                None => {},
            }
        },
    }

    value += incr;
    let obj = RedisObject::String { ptr: StringStorageType::String(value.to_string()) };
    let encoded_obj = try_object_encoding(Arc::new(RwLock::new(obj)));
    c.insert(c.argv[1].read().unwrap().as_key(), encoded_obj.clone());

    c.remove_expire(c.argv[1].read().unwrap().as_key());
    server_write().dirty += 1;
    c.add_reply(COLON.clone());
    c.add_reply(encoded_obj);
    c.add_reply(CRLF.clone());
}

// 
// list
// 

enum ListWhere {
    Head,
    Tail,
}

fn rpush_command(c: &mut RedisClient) {
    push_generic_command(c, ListWhere::Tail);
}

fn lpush_command(c: &mut RedisClient) {
    push_generic_command(c, ListWhere::Head);
}

fn push_generic_command(c: &mut RedisClient, place: ListWhere) {
    let mut len = 0usize;
    match c.lookup_key_write(c.argv[1].read().unwrap().as_key()) {
        None => {
            match handle_clients_waiting_list_push(c, c.argv[1].read().unwrap().as_key(), c.argv[2].clone()) {
                ListWaiting::Waiting => {
                    c.add_reply(C_ONE.clone());
                    return;
                },
                ListWaiting::NoWait => {
                    let mut l = ListStorageType::LinkedList(LinkedList::new());
                    match place {
                        ListWhere::Head => { l.push_front(c.argv[2].clone()); },
                        ListWhere::Tail => { l.push_back(c.argv[2].clone()); },
                    }
                    len = l.len();
                    c.insert(c.argv[1].read().unwrap().as_key(), Arc::new(RwLock::new(RedisObject::List { l })));
                },
            }
        },
        Some(lobj) => {
            match lobj.write().unwrap().list_mut() {
                Some(l_storage) => {
                    match handle_clients_waiting_list_push(c, c.argv[1].read().unwrap().as_key(), c.argv[2].clone()) {
                        ListWaiting::Waiting => {
                            c.add_reply(C_ONE.clone());
                            return;
                        },
                        ListWaiting::NoWait => {
                            match place {
                                ListWhere::Head => { l_storage.push_front(c.argv[2].clone()); },
                                ListWhere::Tail => { l_storage.push_back(c.argv[2].clone()); },
                            }
                            len = l_storage.len();
                        },
                    }
                },
                None => {
                    c.add_reply(WRONG_TYPE_ERR.clone());
                    return;
                },
            }
        },
    }
    server_write().dirty += 1;
    c.add_reply_str(&format!(":{len}\r\n"));
}

enum ListWaiting {
    Waiting,
    NoWait,
}

/// This should be called from any function PUSHing into lists.
/// 'c' is the "pushing client", 'key' is the key it is pushing data against,
/// 'value' is the element pushed.
/// 
/// If the function returns `NoWait` there was no client waiting for a list push
/// against this key.
/// 
/// If the function returns `Waiting` there was a client waiting for a list push
/// against this key, the element was passed to this client thus it's not
/// needed to actually add it to the list and the caller should return asap.
fn handle_clients_waiting_list_push(c: &RedisClient, key: &str, value: Arc<RwLock<RedisObject>>) -> ListWaiting {
    match c.lookup_blocking_key(key) {
        None => { ListWaiting::NoWait },
        Some(l) => {
            let client = l.front().unwrap().write().unwrap();
            client.add_reply_str("*2\r\n");
            client.add_reply_bulk(c.argv[1].clone());
            client.add_reply_bulk(value);
            client.unblock_client_waiting_data();
            ListWaiting::Waiting
        },
    }
}

fn llen_command(c: &mut RedisClient) {
    match c.lookup_key_read_or_reply(c.argv[1].read().unwrap().as_key(), C_ZERO.clone()) {
        Some(v) => {
            match v.read().unwrap().list() {
                Some(l_storage) => { c.add_reply_u64(l_storage.len() as u64); },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn lrange_command(c: &mut RedisClient) {
    let mut start = 0;
    let mut end = 0;
    match (c.argv[2].read().unwrap().as_key().parse(), c.argv[3].read().unwrap().as_key().parse()) {
        (Ok(s), Ok(e)) => {
            start = s;
            end = e;
        },
        _ => {
            log(LogLevel::Warning, &format!("failed to parse args: '{}', '{}'", c.argv[2].read().unwrap().as_key(), c.argv[3].read().unwrap().as_key()));
            return;
        }
    }

    match c.lookup_key_read_or_reply(c.argv[1].read().unwrap().as_key(), NULL_MULTI_BULK.clone()) {
        Some(v) => {
            match v.read().unwrap().list() {
                Some(l_storage) => {
                    let len = l_storage.len();
                    // convert negative indexes
                    if start < 0 { start += len as i32; }
                    if end < 0 { end += len as i32; }
                    if start < 0 { start = 0; }
                    if end < 0 { end = 0; }

                    // indexes sanity checks
                    if start > end || start >= len as i32 {
                        // Out of range start or start > end result in empty list
                        c.add_reply(EMPTY_MULTI_BULK.clone());
                        return;
                    }
                    if end >= len as i32 {
                        end = len as i32 - 1;
                    }
                    let range_len = end - start + 1;

                    // Return the result in form of a multi-bulk reply
                    c.add_reply_str(&format!("*{}\r\n", range_len));
                    let items = l_storage.range(start, end);
                    for e in items {
                        c.add_reply_bulk(Arc::new(RwLock::new(e)));
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn ltrim_command(c: &mut RedisClient) {
    let mut start = 0;
    let mut end = 0;
    match (c.argv[2].read().unwrap().as_key().parse(), c.argv[3].read().unwrap().as_key().parse()) {
        (Ok(s), Ok(e)) => {
            start = s;
            end = e;
        },
        _ => {
            log(LogLevel::Warning, &format!("failed to parse args: '{}', '{}'", c.argv[2].read().unwrap().as_key(), c.argv[3].read().unwrap().as_key()));
            return;
        }
    }

    match c.lookup_key_write_or_reply(c.argv[1].read().unwrap().as_key(), OK.clone()) {
        Some(v) => {
            match v.write().unwrap().list_mut() {
                Some(l_storage) => {
                    let len = l_storage.len();
                    let mut ltrim = 0usize;
                    let mut rtrim = 0usize;
                    // convert negative indexes
                    if start < 0 { start += len as i32; }
                    if end < 0 { end += len as i32; }
                    if start < 0 { start = 0; }
                    if end < 0 { end = 0; }

                    // indexes sanity checks
                    if start > end || start >= len as i32 {
                        ltrim = len;
                        rtrim = 0;
                    } else {
                        if end > len as i32 { end = len as i32; }
                        ltrim = start as usize;
                        rtrim = len - (end as usize);
                    }

                    // Remove list elements to perform the trim
                    l_storage.retain_range(ltrim as i32, rtrim as i32);
                    server_write().dirty += 1;
                    c.add_reply(OK.clone());
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn lindex_command(c: &mut RedisClient) {
    let mut index = 0;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(i) => { index = i; },
        _ => {
            log(LogLevel::Warning, &format!("failed to parse args: '{}'", c.argv[2].read().unwrap().as_key()));
            return;
        }
    }

    match c.lookup_key_read_or_reply(c.argv[1].read().unwrap().as_key(), NULL_BULK.clone()) {
        Some(v) => {
            match v.read().unwrap().list() {
                Some(l_storage) => {
                    if index < 0 {
                        index += l_storage.len() as i32;
                    }
                    match l_storage.index(index) {
                        Some(e) => { c.add_reply_bulk(Arc::new(RwLock::new(e))); },
                        None => { c.add_reply(NULL_BULK.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn lset_command(c: &mut RedisClient) {
    let mut index = 0;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(i) => { index = i; },
        _ => {
            log(LogLevel::Warning, &format!("failed to parse args: '{}'", c.argv[2].read().unwrap().as_key()));
            return;
        }
    }

    match c.lookup_key_write_or_reply(c.argv[1].read().unwrap().as_key(), NO_KEY_ERR.clone()) {
        Some(v) => {
            match v.write().unwrap().list_mut() {
                Some(l_storage) => {
                    // TODO: range checking more strictly
                    if index < 0 {
                        index += l_storage.len() as i32;
                    }
                    match l_storage.set(index, c.argv[3].clone()) {
                        true => {
                            server_write().dirty += 1;
                            c.add_reply(OK.clone());
                        },
                        false => { c.add_reply(OUT_OF_RANGE_ERR.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn lrem_command(c: &mut RedisClient) {
    let mut to_remove = 0;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(i) => { to_remove = i; },
        _ => {
            log(LogLevel::Warning, &format!("failed to parse args: '{}'", c.argv[2].read().unwrap().as_key()));
            return;
        }
    }

    match c.lookup_key_write_or_reply(c.argv[1].read().unwrap().as_key(), C_ZERO.clone()) {
        Some(v) => {
            match v.write().unwrap().list_mut() {
                Some(l_storage) => {
                    let mut from_tail = false;
                    if to_remove < 0 {
                        to_remove = -to_remove;
                        from_tail = true;
                    }
                    let removed = match from_tail {
                        false => { l_storage.remove_head(to_remove, c.argv[3].clone()) },
                        true => { l_storage.remove_tail(to_remove, c.argv[3].clone()) },
                    };
                    c.add_reply_str(&format!(":{}\r\n", removed));
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn lpop_command(c: &mut RedisClient) {
    pop_generic_command(c, ListWhere::Head);
}

fn rpop_command(c: &mut RedisClient) {
    pop_generic_command(c, ListWhere::Tail);
}

fn pop_generic_command(c: &mut RedisClient, place: ListWhere) {
    match c.lookup_key_write_or_reply(c.argv[1].read().unwrap().as_key(), NULL_BULK.clone()) {
        Some(v) => {
            match v.write().unwrap().list_mut() {
                Some(l_storage) => {
                    let ele = match place {
                        ListWhere::Head => { l_storage.pop_front() },
                        ListWhere::Tail => { l_storage.pop_back() },
                    };
                    match ele {
                        Some(v) => {
                            c.add_reply_bulk(Arc::new(RwLock::new(v)));
                            server_write().dirty += 1;
                        },
                        None => { c.add_reply(NULL_BULK.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

/// This is the semantic of this command:
/// RPOPLPUSH srclist dstlist:
///  IF LLEN(srclist) > 0
///    element = RPOP srclist
///    LPUSH dstlist element
///    RETURN element
///  ELSE
///    RETURN nil
///  END
/// END
/// 
/// The idea is to be able to get an element from a list in a reliable way
/// since the element is not just returned but pushed against another list
/// as well. This command was originally proposed by Ezra Zygmuntowicz.
fn rpoplpush_command(c: &mut RedisClient) {
    match c.lookup_key_write_or_reply(c.argv[1].read().unwrap().as_key(), NULL_BULK.clone()) {
        Some(v) => {
            match v.write().unwrap().list_mut() {
                Some(l_storage) => {
                    match l_storage.pop_back() {
                        Some(ele) => {
                            // element type of destination list isn't correct
                            let mut obj: Option<Arc<RwLock<RedisObject>>> = None;
                            match c.lookup_key_write(c.argv[2].read().unwrap().as_key()) {
                                Some(d_obj) => {
                                    if d_obj.read().unwrap().is_list() {
                                        obj = Some(d_obj.clone());
                                    } else {
                                        c.add_reply(WRONG_TYPE_ERR.clone());
                                        return;
                                    }
                                },
                                None => {},
                            }

                            // Add the element to the target list (unless it's directly
                            // passed to some BLPOP-ing client
                            match handle_clients_waiting_list_push(c, c.argv[2].read().unwrap().as_key(), Arc::new(RwLock::new(ele.clone()))) {
                                ListWaiting::NoWait => {
                                    match obj {
                                        None => {
                                            // Create the list if the key does not exist
                                            let mut new_l = ListStorageType::LinkedList(LinkedList::new());
                                            new_l.push_front(Arc::new(RwLock::new(ele.clone())));
                                            c.insert(c.argv[2].read().unwrap().as_key(), Arc::new(RwLock::new(RedisObject::List { l: new_l })));
                                        },
                                        Some(v) => {
                                            match v.write().unwrap().list_mut() {
                                                Some(l_storage) => {
                                                    l_storage.push_front(Arc::new(RwLock::new(ele.clone())));
                                                },
                                                None => { /* impossible */ },
                                            }
                                        },
                                    }
                                },
                                ListWaiting::Waiting => {},
                            }

                            // Send the element to the client as reply as well
                            server_write().dirty += 1;
                            c.add_reply_bulk(Arc::new(RwLock::new(ele.clone())));
                        },
                        None => { c.add_reply(NULL_BULK.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

// 
// set
// 

fn sadd_command(c: &mut RedisClient) {
    let mut set: Option<Arc<RwLock<RedisObject>>> = None;
    let arg_r = c.argv[1].read().unwrap();
    let key = arg_r.as_key();
    match c.lookup_key_write(key) {
        Some(v) => {
            if !v.read().unwrap().is_set() {
                c.add_reply(WRONG_TYPE_ERR.clone());
                return;
            }
            set = Some(v);
        },
        None => {
            let new_set = Arc::new(RwLock::new(RedisObject::Set { s: SetStorageType::HashSet(HashSet::new()) }));
            c.insert(key, new_set.clone());
            set = Some(new_set);
        },
    }

    match set.unwrap().write().unwrap().set_mut() {
        Some(s_storage) => {
            if s_storage.insert(c.argv[2].clone()) {
                server_write().dirty += 1;
                c.add_reply(C_ONE.clone());
            } else {
                c.add_reply(C_ZERO.clone());
            }
        },
        None => { assert!(true, "impossible code"); },
    }
}

fn srem_command(c: &mut RedisClient) {
    let arg_r = c.argv[1].read().unwrap();
    let key = arg_r.as_key();
    match c.lookup_key_write_or_reply(key, C_ZERO.clone()) {
        Some(obj) => {
            match obj.write().unwrap().set_mut() {
                Some(s_storage) => {
                    if s_storage.remove(c.argv[2].clone()) {
                        server_write().dirty += 1;
                        c.add_reply(C_ONE.clone());
                    } else {
                        c.add_reply(C_ZERO.clone());
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn spop_command(c: &mut RedisClient) {
    let arg_r = c.argv[1].read().unwrap();
    let key = arg_r.as_key();
    match c.lookup_key_write_or_reply(key, NULL_BULK.clone()) {
        Some(obj) => {
            match obj.write().unwrap().set_mut() {
                Some(s_storage) => {
                    match s_storage.get_random_key() {
                        Some(ele) => {
                            if s_storage.remove(ele.clone()) {
                                server_write().dirty += 1;
                                c.add_reply_bulk(ele);
                            } else {
                                log(LogLevel::Warning, "failed to remove random element");
                            }
                        },
                        None => { c.add_reply(NULL_BULK.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn smove_command(c: &mut RedisClient) {
    let sarg_r = c.argv[1].read().unwrap();
    let skey = sarg_r.as_key();
    let darg_r = c.argv[2].read().unwrap();
    let dkey = darg_r.as_key();
    match c.lookup_key_write(skey) {
        Some(obj) => {
            match obj.write().unwrap().set_mut() {
                Some(s_storage) => {
                    let mut dset: Option<Arc<RwLock<RedisObject>>> = None;
                    // check destination set type
                    let mut existed = false;
                    match c.lookup_key_write(dkey) {
                        Some(d_obj) => {
                            match d_obj.write().unwrap().set_mut() {
                                Some(_ds_storage) => {
                                    existed = true;
                                    dset = Some(d_obj.clone());
                                },
                                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
                            }
                        },
                        None => {},
                    }

                    if !s_storage.remove(c.argv[3].clone()) {
                        c.add_reply(C_ZERO.clone());
                        return;
                    }

                    if !existed {
                        let new_set = Arc::new(RwLock::new(RedisObject::Set { s: SetStorageType::HashSet(HashSet::new()) }));
                        c.insert(dkey, new_set.clone());
                        dset = Some(new_set);
                    }
                    match dset.unwrap().write().unwrap().set_mut() {
                        Some(ds_storage) => {
                            ds_storage.insert(c.argv[3].clone());
                        },
                        None => { assert!(true, "impossible code"); }
                    }
                    server_write().dirty += 1;
                    c.add_reply(C_ONE.clone());
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => { c.add_reply(C_ZERO.clone()); },
    }
}

fn scard_command(c: &mut RedisClient) {
    let arg_r = c.argv[1].read().unwrap();
    let key = arg_r.as_key();
    match c.lookup_key_read_or_reply(key, C_ZERO.clone()) {
        Some(obj) => {
            match obj.read().unwrap().set() {
                Some(s_storage) => {
                    c.add_reply_u64(s_storage.len() as u64);
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn sismember_command(c: &mut RedisClient) {
    let arg_r = c.argv[1].read().unwrap();
    let key = arg_r.as_key();
    match c.lookup_key_read_or_reply(key, C_ZERO.clone()) {
        Some(obj) => {
            match obj.read().unwrap().set() {
                Some(s_storage) => {
                    if s_storage.contains(c.argv[2].clone()) {
                        c.add_reply(C_ONE.clone());
                    } else {
                        c.add_reply(C_ZERO.clone());
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn sinter_command(c: &mut RedisClient) {
    sinter_generic_command(c, 1, None);
}

fn sinterstore_command(c: &mut RedisClient) {
    sinter_generic_command(c, 2, Some(c.argv[1].clone()));
}

fn sinter_generic_command(c: &mut RedisClient, idx: usize, dst: Option<Arc<RwLock<RedisObject>>>) {
    let mut sets: Vec<Arc<RwLock<RedisObject>>> = Vec::new();

    for i in idx..c.argv.len() {
        let arg_r = c.argv[i].read().unwrap();
        let key = arg_r.as_key();
        let mut set_obj = c.lookup_key_read(key);
        if dst.is_some() {
            set_obj = c.lookup_key_write(key);
        }

        match set_obj {
            Some(s_obj) => {
                match s_obj.read().unwrap().set() {
                    Some(_) => { sets.push(s_obj.clone()); },
                    None => {
                        c.add_reply(WRONG_TYPE_ERR.clone());
                        return;
                    },
                }
            },
            None => {
                match dst {
                    Some(ref dkey) => {
                        match dkey.write().unwrap().set_mut() {
                            Some(_) => {
                                if c.remove(c.argv[i].read().unwrap().as_key()).is_some() {
                                    server_write().dirty += 1;
                                }
                                c.add_reply(C_ZERO.clone());
                            },
                            None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
                        }
                    },
                    None => { c.add_reply(NULL_MULTI_BULK.clone()); },
                }
                return;
            },
        }
    }

    // Sort sets from the smallest to largest, this will improve our algorithm's performace
    sets.sort_by(|a, b| {
        a.read().unwrap().set().unwrap().len().cmp(&b.read().unwrap().set().unwrap().len())
    });

    // Iterate all the elements of the first (smallest) set, and test
    // the element against all the other sets, if at least one set does
    // not include the element it is discarded
    let set0_r = sets[0].read().unwrap();
    let mut iter = set0_r.set().unwrap().iter();
    let mut acc: HashSet<RedisObject> = HashSet::new();
    let mut j = 0usize;
    while let Some(ele) = iter.next() {
        j = 1;
        while j < sets.len() {
            if !sets[j].read().unwrap().set().unwrap().contains2(ele) { break; }
            j += 1;
        }
        if j != sets.len() { continue; }
        acc.insert(ele.clone());
    }

    let len = acc.len();
    match dst {
        Some(dkey) => {
            c.delete_key(dkey.read().unwrap().as_key());
            let new_s = Arc::new(RwLock::new(RedisObject::Set { s: SetStorageType::HashSet(acc) }));
            c.insert(dkey.read().unwrap().as_key(), new_s);

            server_write().dirty += 1;
            c.add_reply_str(&format!(":{}\r\n", len));
        },
        None => {
            c.add_reply_str(&format!("*{}\r\n", len));
            for ele in &acc {
                c.add_reply_bulk(Arc::new(RwLock::new(ele.clone())));
            }
        },
    }
}

#[derive(PartialEq)]
enum SetOp {
    Union,
    Diff,
    Inter,
}

fn sunion_command(c: &mut RedisClient) {
    sunion_diff_generic_command(c, 1, None, SetOp::Union);
}

fn sunionstore_command(c: &mut RedisClient) {
    sunion_diff_generic_command(c, 2, Some(c.argv[1].clone()), SetOp::Union);
}

fn sdiff_command(c: &mut RedisClient) {
    sunion_diff_generic_command(c, 1, None, SetOp::Diff);
}

fn sdiffstore_command(c: &mut RedisClient) {
    sunion_diff_generic_command(c, 2, Some(c.argv[1].clone()), SetOp::Diff);
}

fn sunion_diff_generic_command(c: &mut RedisClient, idx: usize, dst: Option<Arc<RwLock<RedisObject>>>, op: SetOp) {
    let mut sets: Vec<Option<Arc<RwLock<RedisObject>>>> = Vec::new();

    for i in idx..c.argv.len() {
        let arg_r = c.argv[i].read().unwrap();
        let key = arg_r.as_key();
        let mut set_obj = c.lookup_key_read(key);
        if dst.is_some() {
            set_obj = c.lookup_key_write(key);
        }

        match set_obj {
            Some(s_obj) => {
                match s_obj.read().unwrap().set() {
                    Some(_) => { sets.push(Some(s_obj.clone())); },
                    None => {
                        c.add_reply(WRONG_TYPE_ERR.clone());
                        return;
                    },
                }
            },
            None => { sets.push(None); },
        }
    }

    let mut acc: HashSet<RedisObject> = HashSet::new();
    let mut cardinality = 0;
    for i in 0..sets.len() {
        if op == SetOp::Diff && i == 0 && sets[i].is_none() { break; }
        if sets[i].is_none() { continue; }

        let set_r = sets[i].as_ref().unwrap().read().unwrap();
        let mut iter = set_r.set().unwrap().iter();
        while let Some(ele) = iter.next() {
            if op == SetOp::Union || i == 0 {
                if acc.insert(ele.clone()) {
                    cardinality += 1;
                }
            } else if op == SetOp::Diff {
                if acc.remove(ele) {
                    cardinality -= 1;
                }
            }
        }

        if op == SetOp::Diff && cardinality == 0 {
            break;
        }
    }

    match dst {
        Some(dkey) => {
            c.delete_key(dkey.read().unwrap().as_key());
            let new_s = Arc::new(RwLock::new(RedisObject::Set { s: SetStorageType::HashSet(acc) }));
            c.insert(dkey.read().unwrap().as_key(), new_s);

            server_write().dirty += 1;
            c.add_reply_str(&format!(":{}\r\n", cardinality));
        },
        None => {
            c.add_reply_str(&format!("*{}\r\n", cardinality));
            for ele in &acc {
                c.add_reply_bulk(Arc::new(RwLock::new(ele.clone())));
            }
        },
    }
}

fn srandmember_command(c: &mut RedisClient) {
    let arg_r = c.argv[1].read().unwrap();
    let key = arg_r.as_key();
    match c.lookup_key_read_or_reply(key, NULL_BULK.clone()) {
        Some(obj) => {
            match obj.read().unwrap().set() {
                Some(s_storage) => {
                    match s_storage.get_random_key() {
                        Some(ele) => {
                            c.add_reply_bulk(ele);
                        },
                        None => { c.add_reply(NULL_BULK.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

// 
// sorted set
// 

fn zadd_command(c: &mut RedisClient) {
    let mut score = 0f64;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(f) => { score = f; },
        Err(_) => {
            log(LogLevel::Warning, &format!("failed to parse score: '{}'", c.argv[2].read().unwrap().as_key()));
            return;
        },
    }

    let key = c.argv[1].read().unwrap().as_key().to_string();
    let obj = c.argv[3].clone();
    zadd_generic_command(c, &key, obj, score, false);
}

fn zrem_command(c: &mut RedisClient) {
    match c.lookup_key_write_or_reply(c.argv[1].read().unwrap().as_key(), C_ZERO.clone()) {
        Some(z_obj) => {
            match z_obj.write().unwrap().zset_mut() {
                Some(zs_storage) => {
                    match zs_storage.dict().get(&c.argv[2].read().unwrap()).cloned() {
                        Some(old_score) => {
                            zs_storage.skiplist_mut().delete(old_score, Arc::new(c.argv[2].read().unwrap().clone()));
                            zs_storage.dict_mut().remove(&c.argv[2].read().unwrap());
                            server_write().dirty += 1;
                            c.add_reply(C_ONE.clone());
                        },
                        None => { c.add_reply(C_ZERO.clone()); },
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

/// This generic command implements both ZADD and ZINCRBY.
/// `score_val` is the score if the operation is a ZADD (do_incr == false) or
/// the increment if the operation is a ZINCRBY (do_incr == true).
fn zadd_generic_command(c: &mut RedisClient, key: &str, obj: Arc<RwLock<RedisObject>>, score_val: f64, do_incr: bool) {
    let zset = match c.lookup_key_write(key) {
        Some(z_obj) => {
            match z_obj.read().unwrap().zset() {
                Some(_) => {},
                None => {
                    c.add_reply(WRONG_TYPE_ERR.clone());
                    return;
                },
            }
            z_obj
        },
        None => {
            let new_zset = Arc::new(RwLock::new(RedisObject::ZSet { zs: ZSetStorageType::SkipList(HashMap::new(), SkipList::new()) }));
            c.insert(key, new_zset.clone());
            new_zset
        },
    };

    let mut score = score_val;
    if do_incr {
        match zset.read().unwrap().zset().unwrap().dict().get(&obj.read().unwrap()) {
            Some(old_s) => { score += old_s; },
            None => {},
        }
    }

    let mut zset_w = zset.write().unwrap();
    let ele = Arc::new(obj.read().unwrap().clone());
    match zset_w.zset_mut().unwrap().dict_mut().insert(obj.read().unwrap().clone(), score) {
        None => {
            zset_w.zset_mut().unwrap().skiplist_mut().insert(score, ele.clone());
            server_write().dirty += 1;
            if do_incr {
                c.add_reply_f64(score);
            } else {
                c.add_reply(C_ONE.clone());
            }
        },
        Some(old_s) => {
            if old_s != score {
                zset_w.zset_mut().unwrap().skiplist_mut().delete(old_s, ele.clone());
                zset_w.zset_mut().unwrap().skiplist_mut().insert(score, ele.clone());
            }
            if do_incr {
                c.add_reply_f64(score);
            } else {
                c.add_reply(C_ZERO.clone());
            }
        },
    }
}

fn zincrby_command(c: &mut RedisClient) {
    let mut incr = 0f64;
    match c.argv[2].read().unwrap().as_key().parse() {
        Ok(f) => { incr = f; },
        Err(_) => {
            log(LogLevel::Warning, &format!("failed to parse incr: '{}'", c.argv[2].read().unwrap().as_key()));
            return;
        },
    }

    let key = c.argv[1].read().unwrap().as_key().to_string();
    let obj = c.argv[3].clone();
    zadd_generic_command(c, &key, obj, incr, true);
}

fn zrange_command(c: &mut RedisClient) {
    zrange_generic_command(c, false);
}

fn zrevrange_command(c: &mut RedisClient) {
    zrange_generic_command(c, true);
}

fn zrange_generic_command(c: &mut RedisClient, reverse: bool) {
    let mut start = 0i32;
    let mut end = 0i32;
    match (c.argv[2].read().unwrap().as_key().parse(), c.argv[3].read().unwrap().as_key().parse()) {
        (Ok(s), Ok(e)) => {
            start = s;
            end = e;
        },
        _ => {
            log(LogLevel::Warning, &format!("failed to parse args: '{}', '{}'", c.argv[2].read().unwrap().as_key(), c.argv[3].read().unwrap().as_key()));
            return;
        }
    }

    let mut with_score = false;
    if c.argv.len() == 5 && c.argv[4].read().unwrap().as_key().eq_ignore_ascii_case("withscores") {
        with_score = true;
    } else if c.argv.len() >= 5 {
        c.add_reply(SYNTAX_ERR.clone());
        return;
    }

    match c.lookup_key_read_or_reply(c.argv[1].read().unwrap().as_key(), NULL_MULTI_BULK.clone()) {
        Some(z_obj) => {
            match z_obj.read().unwrap().zset() {
                Some(zs_storage) => {
                    let zsl = zs_storage.skiplist();
                    let len = zsl.len();
                    // convert negative indexes
                    if start < 0 { start += len as i32; }
                    if end < 0 { end += len as i32; }
                    if start < 0 { start = 0; }
                    if end < 0 { end = 0; }

                    // indexes sanity checks
                    if start > end || start >= len as i32 {
                        c.add_reply(EMPTY_MULTI_BULK.clone());
                        return;
                    }
                    if end >= len as i32 { end = len as i32; }
                    let range_len = end - start;

                    let mut ln = match reverse {
                        true => match start == 0 {
                            true => zsl.tail(),
                            false => zsl.get_ele_by_rank(len - start as usize),
                        },
                        false => match start == 0 {
                            true => zsl.header(0),
                            false => zsl.get_ele_by_rank(start as usize + 1),
                        },
                    };

                    match with_score {
                        true => c.add_reply_str(&format!("*{}\r\n", 2 * range_len)),
                        false => c.add_reply_str(&format!("*{}\r\n", range_len)),
                    };
                    for _ in 0..range_len {
                        let node = ln.clone().unwrap();
                        let obj = node.read().unwrap().obj();
                        c.add_reply_bulk(Arc::new(RwLock::new(obj.unwrap().deref().clone())));
                        if with_score {
                            c.add_reply_f64(node.read().unwrap().score());
                        }
                        ln = match reverse {
                            true => { node.read().unwrap().backward() },
                            false => { node.read().unwrap().forward(0) },
                        };
                    }
                },
                None => { c.add_reply(WRONG_TYPE_ERR.clone()); },
            }
        },
        None => {},
    }
}

fn zrangebyscore_command(c: &mut RedisClient) {
    
}

fn zcard_command(c: &mut RedisClient) {
    
}

fn zscore_command(c: &mut RedisClient) {
    
}

fn zremrangebyscore_command(c: &mut RedisClient) {
    
}

fn sort_command(c: &mut RedisClient) {
    
}

fn save_command(c: &mut RedisClient) {
    
}

fn bgsave_command(c: &mut RedisClient) {
    
}

fn lastsave_command(c: &mut RedisClient) {
    
}

fn shutdown_command(c: &mut RedisClient) {
    
}

fn bgrewriteaof_command(c: &mut RedisClient) {
    
}

fn info_command(c: &mut RedisClient) {
    
}

fn monitor_command(c: &mut RedisClient) {
    
}

fn slaveof_command(c: &mut RedisClient) {
    
}
