use std::{borrow::BorrowMut, collections::HashMap, ops::{BitOr, Deref}, sync::Arc};
use once_cell::sync::Lazy;
use crate::{redis::obj::{NULL_BULK, PONG, WRONG_TYPE_ERR}, util::{log, LogLevel}};
use super::{client::RedisClient, obj::{RedisObject, C_ONE, OK}, server_write};


/// 
/// Redis Commands.
/// 


pub static MAX_SIZE_INLINE_CMD: usize = 1024 * 1024 * 256;  // max bytes in inline command


/// Command Table 
static CMD_TABLE: Lazy<HashMap<&str, Arc<RedisCommand>>> = Lazy::new(|| {
    HashMap::from([
        ("get", Arc::new(RedisCommand { name: "get", proc: Arc::new(get_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 })),
        ("set", Arc::new(RedisCommand { name: "set", proc: Arc::new(set_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("ping", Arc::new(RedisCommand { name: "ping", proc: Arc::new(ping_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("exec", Arc::new(RedisCommand { name: "exec", proc: Arc::new(exec_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
        ("discard", Arc::new(RedisCommand { name: "discard", proc: Arc::new(discard_command), arity: 1, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 })),
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

fn get_command(c: &mut RedisClient) {
    match get_generic_command(c) {
        Ok(_) => {},
        Err(e) => {
            log(LogLevel::Warning, &e);
        },
    }
}
fn get_generic_command(c: &mut RedisClient) -> Result<(), String> {
    let key = c.argv[1].string().unwrap().string().unwrap().to_string();
    match c.lookup_key_read_or_reply(&key, NULL_BULK.clone()) {
        None => Ok(()),
        Some(v) => {
            match v.deref() {
                RedisObject::String { ptr: _ } => {
                    c.add_reply_bulk(v);
                    Ok(())
                },
                _ => {
                    c.add_reply(WRONG_TYPE_ERR.clone());
                    Err("WRONG TYPE ERROR".to_string())
                },
            }
        },
    }
}

fn set_command(c: &mut RedisClient) {
    set_generic_command(c, false);
}
fn set_generic_command(c: &mut RedisClient, nx: bool) {
    if nx {
        // TODO: delete if volatile
    }

    let key = c.argv[1].string().unwrap().string().unwrap();
    let db = c.db.clone().expect("db doesn't exist");
    let mut db_w = db.write().unwrap();
    db_w.dict.insert(key.to_string(), c.argv[2].clone());
    // TODO: if failed to insert

    server_write().dirty += 1;
    db_w.remove_expire(key);
    if nx {
        c.add_reply(C_ONE.clone());
    } else {
        c.add_reply(OK.clone());
    }
}

pub fn exec_command(c: &mut RedisClient) {
    todo!()
}

pub fn discard_command(c: &mut RedisClient) {
    todo!()
}

