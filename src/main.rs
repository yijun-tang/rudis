use std::{env, process::exit, sync::Arc, time::Instant};
use rredis::redis::{before_sleep, log::LogLevel, RedisServer, REDIS_VERSION};

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut server = Box::new(RedisServer::new());

    if args.len() == 2 {
        server.reset_server_save_params();
        server.load_server_config(&args[1]);
    } else if args.len() > 2 {
        eprintln!("Usage: ./redis-server [/path/to/redis.conf]");
        exit(1);
    } else {
        server.log(LogLevel::Warning, "Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'");
    }
    if server.is_daemonize() {
        server.daemonize();
    }

    server.init_server();
    server.log(LogLevel::Notice, &format!("Server started, Redis version {}", REDIS_VERSION));

    #[cfg(target_os = "linux")]
    server.linux_overcommit_memory_warning();

    /* let start = Instant::now();
    if server.append_only() {
        if let Ok(_) = server.load_append_only_file() {
            server.log(LogLevel::Notice, &format!("DB loaded from append only file: {} seconds", start.elapsed().as_secs()));
        }
    } else {
        if let Ok(_) = server.rdb_load() {
            server.log(LogLevel::Notice, &format!("DB loaded from disk: {} seconds", start.elapsed().as_secs()));
        }
    } */

    server.log(LogLevel::Notice, &format!("The server is now ready to accept connections on port {}", server.port()));
    server.set_before_sleep_proc(Some(Arc::new(before_sleep)));
    server.main();
}
