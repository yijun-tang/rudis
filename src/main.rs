use rredis::{
    aof::load_append_only_file, eventloop::{ae_main, set_before_sleep_proc}, handler::before_sleep, rdb::rdb_load, server::{print_logo, server_read, server_write}, util::{log, LogLevel}
};
use std::{env, process::exit, sync::Arc, time::Instant};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 {
        server_write().reset_server_save_params();
        server_write().load_server_config(&args[1]);
    } else if args.len() > 2 {
        eprintln!("Usage: ./redis-server [/path/to/redis.conf]");
        exit(1);
    } else {
        log(LogLevel::Warning, "Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'");
    }
    if server_read().is_daemonize() {
        server_read().daemonize();
    }

    server_write().init_server();
    print_logo();

    #[cfg(target_os = "linux")]
    server_read().linux_overcommit_memory_warning();

    let start = Instant::now();
    if server_read().append_only() {
        let filename = { server_read().append_filename().to_string() };
        if let Ok(_) = load_append_only_file(&filename) {
            log(LogLevel::Notice, &format!("DB loaded from append only file: {} seconds", start.elapsed().as_secs()));
        }
    } else {
        let file = server_read().db_filename().to_string();
        if rdb_load(&file) {
            log(LogLevel::Notice, &format!("DB loaded from disk: {} seconds", start.elapsed().as_secs()));
        }
    }

    log(
        LogLevel::Notice,
        &format!(
            "The server is now ready to accept connections on port {}",
            server_read().port()
        ),
    );
    set_before_sleep_proc(Some(Arc::new(before_sleep)));
    ae_main();
}
