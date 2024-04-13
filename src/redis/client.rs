use std::{collections::LinkedList, sync::Arc};
use crate::redis::log::LogLevel;
use super::{RedisDB, RedisServer, ReplState};

pub struct ClientFlags(u8);

impl ClientFlags {
    /// This client is a slave server
    fn slave() -> ClientFlags {
        ClientFlags(1)
    }
    /// This client is a master server
    fn master() -> ClientFlags {
        ClientFlags(2)
    }
    /// This client is a slave monitor, see MONITOR
    fn monitor() -> ClientFlags {
        ClientFlags(4)
    }
    /// This client is in a MULTI context
    fn multi() -> ClientFlags {
        ClientFlags(8)
    }
    /// The client is waiting in a blocking operation
    fn blocked() -> ClientFlags {
        ClientFlags(16)
    }
    /// The client is waiting for Virtual Memory I/O
    fn io_wait() -> ClientFlags {
        ClientFlags(32)
    }
}

/// With multiplexing we need to take per-clinet state.
/// Clients are taken in a liked list.
pub struct RedisClient {
    fd: i32,
    db: Option<Arc<RedisDB>>,
    query_buf: String,
    argv: Vec<String>,
    reply: LinkedList<String>,
    flags: ClientFlags,
    repl_state: ReplState,          // replication state if this is a slave
}

impl RedisClient {
    /// In Redis commands are always executed in the context of a client, so in
    /// order to load the append only file we need to create a fake client.
    pub fn create_fake_client(server: &RedisServer) -> RedisClient {
        let mut c = RedisClient { 
            db: None, 
            fd: -1, 
            query_buf: String::new(),
            argv: Vec::new(),
            flags: ClientFlags(0),
            // We set the fake client as a slave waiting for the synchronization
            // so that Redis will not try to send replies to this client.
            repl_state: ReplState::WaitBgSaveStart,
            reply: LinkedList::new(),
        };

        c.select_db(0, server);
        c
    }

    pub fn set_argv(&mut self, argv: Vec<String>) {
        self.argv = argv;
    }

    fn select_db(&mut self, id: i32, server: &RedisServer) {
        if id < 0 || id >= server.dbnum {
            server.log(LogLevel::Warning, &format!("Invalid db #{} out of [0, {})", id, server.dbnum));
            return;
        }
        self.db = Some(server.dbs[id as usize].clone());
    }
}
