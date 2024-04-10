use std::{env, process::exit, rc::Rc, time::Instant};

use rredis::{redis::{before_sleep, daemonize, init_server, load_append_only_file, rdb_load, RedisServer, REDIS_VERSION}, util::LogLevel};

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
        // macro: redis_log!
        // redis_log(LogLevel::Warning, "Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'");
    }
    if server.daemonize() {
        daemonize();
    }

    init_server();
    // redis_log(LogLevel::Notice, "Server started, Redis version ", REDIS_VERSION);

    // Conditional Compilation
    // #ifdef __linux__
    // linuxOvercommitMemoryWarning();
    // #endif

    let start = Instant::now();
    if server.append_only() {
        if let Ok(_) = load_append_only_file(server.append_filename()) {
            // redis_log(LogLevel::Notice, "DB loaded from append only file: {} seconds", start.elapsed().as_secs());
        }
    } else {
        if let Ok(_) = rdb_load(server.db_filename()) {
            // redis_log(LogLevel::Notice, "DB loaded from disk: {} seconds", start.elapsed().as_secs());
        }
    }

    // redis_log(LogLevel::Notice, "The server is now ready to accept connections on port {}", server.port());
    server.set_before_sleep_proc(Some(Rc::new(before_sleep)));
    server.main();
}
