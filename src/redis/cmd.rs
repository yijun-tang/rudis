use std::{collections::HashMap, ops::BitOr, sync::Arc};
use once_cell::sync::Lazy;
use super::client::RedisClient;

static CMD_TABLE: Lazy<HashMap<&str, RedisCommand>> = Lazy::new(|| {
    HashMap::from([
        ("get", RedisCommand { name: "get", proc: Arc::new(get_command), arity: 2, flags: CmdFlags::inline(), vm_preload_proc: None, vm_firstkey: 1, vm_lastkey: 1, vm_keystep: 1 }),
        ("set", RedisCommand { name: "set", proc: Arc::new(set_command), arity: 3, flags: CmdFlags::bulk() | CmdFlags::deny_oom(), vm_preload_proc: None, vm_firstkey: 0, vm_lastkey: 0, vm_keystep: 0 }),
    ])
});

/// Command flags
struct CmdFlags(u8);

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
}

impl BitOr for CmdFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        CmdFlags(self.0 | rhs.0)
    }
}

type CommandProc = Arc<dyn Fn(&mut RedisClient) -> () + Sync + Send>;

struct RedisCommand {
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

fn lookup_command(name: &str) -> &RedisCommand {
    todo!()
}

fn get_command(c: &mut RedisClient) {
    get_generic_command(c);
}

fn get_generic_command(c: &mut RedisClient) -> i32 {

    todo!()
}

fn set_command(c: &mut RedisClient) {
    set_generic_command(c, 0);
}

fn set_generic_command(c: &mut RedisClient, nx: i32) {
    todo!()
}
