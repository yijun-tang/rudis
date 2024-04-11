use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn timestamp() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub fn yes_no_to_bool(s: &str) -> Result<bool, String> {
    match &s.to_ascii_lowercase()[..] {
        "yes" => { Ok(true) },
        "no" => { Ok(false) },
        _ => { Err("argument must be 'yes' or 'no'".to_string()) },
    }
}
