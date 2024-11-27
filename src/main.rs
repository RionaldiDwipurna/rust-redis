#![allow(unused_imports)]
use core::panic;
use core::str;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::read;
use std::hash::Hash;
use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::u64;

struct RedisConfig {
    config: HashMap<String, String>,
}

impl RedisConfig {
    fn parse_argument(mut args: Vec<String>) -> Self {
        args.remove(0);
        let mut config = HashMap::new();
        let mut args_iter = args.iter().peekable();
        let flags = ["--dir", "--dbfilename"];

        while let Some(arg) = args_iter.next() {
            if flags.contains(&arg.as_str()) {
                match args_iter.peek() {
                    Some(&next_args) if flags.contains(&next_args.as_str()) => {
                        panic!("Missing value for the flag");
                    }
                    Some(&next_args) => {
                        //let mut string_final = next_args.clone();
                        //
                        //if arg.as_str() == "--dir" && !next_args.ends_with("/") {
                        //    string_final.push('/');
                        //}

                        config.insert(arg.clone(), next_args.clone());
                    }

                    None => {
                        panic!("Missing value for the flag");
                    }
                }
            }
        }
        println!("{:?}", config);
        return Self { config };
    }

    fn set_config(&mut self, command: &RedisCommand) {}

    fn get_config(&self, command: &RedisCommand) -> String {
        // config get dir

        let key = command.str_cmd[2].clone(); // get the key

        let value = match self.config.get(&format!("--{}", &key)) {
            Some(val) => val.clone(),
            None => "".to_string(),
        };

        let lst_str = vec![key, value];
        let base_str = format!("*{}\r\n", lst_str.len());

        let formatted_item = lst_str
            .iter()
            .map(|item| format!("${}\r\n{}\r\n", item.len(), item))
            .collect::<String>();

        return base_str + &formatted_item;
    }
}

enum RedisResponse {
    OK(String),
    Error(String),
}

impl RedisResponse {
    fn to_string(&self) -> String {
        match self {
            RedisResponse::OK(message) => format!("+{}\r\n", message),
            RedisResponse::Error(message) => format!("-{}\r\n", message),
        }
    }
}

//#[derive(Debug)]
struct RedisCommand {
    cmd_len: i32,          // Number of commands (e.g., echo hey -> 2)
    n_chars_len: Vec<i32>, // Length of each argument (e.g., [4, 3])
    str_cmd: Vec<String>,  // Command arguments (e.g., ["ECHO", "hey"])
}

impl RedisCommand {
    fn parser_receive(command_str: &str) -> Result<Self, String> {
        let mut lines = command_str.split("\r\n").filter(|x| !x.is_empty());

        //let mut cmd_len = 0;
        let mut n_chars_len = Vec::new();
        let mut str_cmd = Vec::new();
        let cmd_len = if let Some(line) = lines.next() {
            if line.starts_with('*') {
                line[1..]
                    .parse::<i32>()
                    .expect("Failed to parse command length")
            } else {
                return Err("Invalid command format: expected '*'".to_string());
            }
        } else {
            return Err("Command string is empty".to_string());
        };

        while let Some(line) = lines.next() {
            match line.chars().next() {
                Some('$') => {
                    n_chars_len.push(
                        line.trim_start_matches('$')
                            .parse::<i32>()
                            .expect("Failed to parse character length"),
                    );
                }

                _ => str_cmd.push(line.to_lowercase()),
            }
        }

        Ok(Self {
            cmd_len,
            n_chars_len,
            str_cmd,
        })
    }

    fn format_response_code(&self, input: Option<String>) -> String {
        let chars_len: Vec<i32>;
        let str_cmd_to_use = match input {
            Some(s) => {
                chars_len = vec![0, s.len() as i32];
                vec![s]
            }
            None => {
                chars_len = self.n_chars_len.clone();
                self.str_cmd[1..].to_vec()
            }
        };

        str_cmd_to_use
            .iter()
            .enumerate()
            .map(|(i, item)| format!("${}\r\n{}\r\n", chars_len[i + 1], item))
            .collect()
    }
}

struct RedisData {
    data: HashMap<String, String>,
    expiry: HashMap<String, std::time::SystemTime>,
}

impl RedisData {
    fn init_db() -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    fn decode_length(&self, content: &Vec<u8>, cursor: &mut usize) -> Option<usize> {
        let first_byte = content[*cursor];
        *cursor += 1;
        //return Some(first_byte as usize);

        match first_byte >> 6 {
            0b00 => Some((first_byte & 0x3F) as usize), // 6-bit length
            0b01 => {
                if *cursor < content.len() {
                    let second_byte = content[*cursor];
                    *cursor += 1;
                    Some((((first_byte & 0x3F) as usize) << 8) | (second_byte as usize))
                // 14-bit length
                } else {
                    None
                }
            }
            0b10 => {
                if *cursor + 3 < content.len() {
                    let mut length_bytes = [0u8; 4];
                    length_bytes.copy_from_slice(&content[*cursor..*cursor + 4]);
                    *cursor += 4;
                    Some(u32::from_be_bytes(length_bytes) as usize) // 32-bit length
                } else {
                    None
                }
            }
            0b11 => {
                // Special encoding for integers or other data, handle separately
                Some((first_byte & 0x3F) as usize) // You may need to modify for specific cases
            }
            _ => None,
        }
    }

    fn read_values(&self, content: &Vec<u8>, cursor: &mut usize) -> Option<(String, String)> {
        match content[*cursor] {
            0x00 => {
                // string
                *cursor += 1;
                let keys = self
                    .read_string(content, cursor)
                    .expect("Error reading the keys");
                let values = self
                    .read_string(content, cursor)
                    .expect("Error reading the values");
                Some((keys, values))
            }
            // other values to be implemented
            _ => None,
        }
    }

    fn read_string(&self, content: &Vec<u8>, cursor: &mut usize) -> Option<String> {
        let length = self.decode_length(content, cursor)?;
        if *cursor + length <= content.len() {
            let string_data = &content[*cursor..*cursor + length];
            *cursor += length;
            Some(String::from_utf8_lossy(string_data).to_string())
        } else {
            None
        }
    }

    fn parse_db_key_val(&mut self, content: &Vec<u8>, cursor: &mut usize) {
        *cursor += 1;
        let size_table_all = content[*cursor];
        println!("Table size all: {}", size_table_all);

        *cursor += 1;
        let size_table_expired = content[*cursor];
        println!("Table size expired: {}", size_table_expired);

        let size_table_no_expired = size_table_all - size_table_expired;

        *cursor += 1;
        //let tmp_cursor = cursor.clone();
        for _ in 0..size_table_all {
            match content[*cursor] {
                0xFC => {
                    *cursor += 1; // move 1 step toward the time stamp
                    let mut timestamp = content[*cursor..*cursor + 8].to_vec();
                    *cursor += 8; //skip the timestamp
                    timestamp.reverse();
                    let timestamp_ms = u64::from_be_bytes(timestamp.try_into().unwrap());
                    let time = UNIX_EPOCH + Duration::from_millis(timestamp_ms);
                    let (keys, values) = self
                        .read_values(content, cursor)
                        .expect("Error reading the keys and values");
                    self.data.insert(keys.clone(), values.clone());
                    self.expiry.insert(keys.clone(), time);
                    println!(
                        "Key: {}, Value: {}, timestamp: {}",
                        keys, values, timestamp_ms
                    );
                }

                0xFD => {
                    *cursor += 1;
                    let mut timestamp = content[*cursor..*cursor + 4].to_vec();
                    *cursor += 8;
                    timestamp.reverse();
                    let timestamp_sec = u64::from_be_bytes(timestamp.try_into().unwrap());
                    let time = UNIX_EPOCH + Duration::from_secs(timestamp_sec);
                    println!("timestamp: {}", timestamp_sec);
                    let (keys, values) = self
                        .read_values(content, cursor)
                        .expect("Error reading the keys and values");

                    self.data.insert(keys.clone(), values.clone());
                    self.expiry.insert(keys.clone(), time);
                    println!(
                        "Key: {}, Value: {}, timestamp: {}",
                        keys, values, timestamp_sec
                    );
                }

                _ => {
                    let (keys, values) = self
                        .read_values(content, cursor)
                        .expect("Error reading the keys and values");
                    self.data.insert(keys.clone(), values.clone());
                    println!("Key: {}, Value: {}", keys, values);
                }
            }
        }
    }

    fn read_from_file(&mut self, rconfig: &RedisConfig) -> RedisResponse {
        let mut dir = match rconfig.config.get("--dir") {
            Some(val) => val.clone(),
            None => {
                return RedisResponse::Error("No directory!".to_string());
            }
        };

        let file = match rconfig.config.get("--dbfilename") {
            Some(val) => val,
            None => {
                return RedisResponse::Error("No file!".to_string());
            }
        };

        if !dir.ends_with("/") {
            dir.push('/');
        }

        let file_path = dir.clone() + file;

        let content = match fs::read(&file_path) {
            Ok(file) => file,
            Err(e) => {
                //eprintln!("Error: {e}");
                return RedisResponse::Error(format!("{}", e));
            }
        };
        let mut cursor = 0;
        let mut in_rdb_content = false;

        if str::from_utf8(&content[0..5]).unwrap() != "REDIS" {
            return RedisResponse::Error("Magic string failed".to_string());
        }

        while cursor < content.len() {
            match content[cursor] {
                0xFA => {
                    cursor += 1;
                }

                0xFB => {
                    self.parse_db_key_val(&content, &mut cursor);
                }

                0xFE => {
                    cursor += 1;
                }

                0xFF => {
                    break;
                }

                _ => {
                    cursor += 1;
                }
            }
        }

        return RedisResponse::OK("OK".to_string());
    }

    fn set_value(&mut self, command: &RedisCommand) -> RedisResponse {
        self.data
            .insert(command.str_cmd[1].clone(), command.str_cmd[2].clone());
        if command.cmd_len == 5 && command.str_cmd[3].as_str() == "px" {
            self.expiry.insert(
                command.str_cmd[1].clone(),
                SystemTime::now()
                    + Duration::from_millis(
                        command.str_cmd[4]
                            .parse::<u64>()
                            .expect("Parsing time failed!"),
                    ),
            );
        }

        RedisResponse::OK(String::from("OK"))
    }

    fn get_value(&mut self, command: &RedisCommand) -> Option<String> {
        let key = &command.str_cmd[1];

        if let Some(&expiry_time) = self.expiry.get(key) {
            if SystemTime::now() > expiry_time {
                self.data.remove(key);
                self.expiry.remove(key);
                return None;
            }
        }

        match self.data.get(key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    fn get_all_keys(&self) -> Option<Vec<String>> {
        let keys = self.data.clone().into_keys().collect::<Vec<String>>();
        if keys.len() == 0 {
            None
        } else {
            Some(keys)
        }
    }
}

fn main() {
    //env::set_var("RUST_BACKTRACE", "full");
    let args: Vec<String> = env::args().collect();
    let config_struct = RedisConfig::parse_argument(args);
    let mut redis_data = RedisData::init_db();

    let read_file = redis_data.read_from_file(&config_struct);

    let config_settings = Arc::new(RwLock::new(config_struct));
    let db_instances = Arc::new(RwLock::new(redis_data));

    println!("Logs from your program will appear here!");

    let mut handles = vec![];

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let db_instances = Arc::clone(&db_instances);
        let config_settings = Arc::clone(&config_settings);

        let handle = thread::spawn(move || match stream {
            Ok(mut stream) => {
                println!("connected");
                //let mut redis_db = db_instances.lock().unwrap();
                event_handler(&mut stream, db_instances, config_settings);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn event_handler(
    stream: &mut TcpStream,
    db_instances: Arc<RwLock<RedisData>>,
    config_settings: Arc<RwLock<RedisConfig>>,
) {
    let mut buf = [0; 1024];
    loop {
        let reader = stream.read(&mut buf).unwrap();

        let readable = match str::from_utf8(&buf) {
            Ok(v) => v,
            Err(e) => panic!("Invalid utf8 {}", e),
        }
        .trim_end_matches('\0');

        let command = RedisCommand::parser_receive(&readable)
            .expect("Error when trying to parse the command");

        if let Some(first_cmd) = command.str_cmd.get(0) {
            match first_cmd.as_str() {
                "echo" => {
                    let final_str = command.format_response_code(None);
                    stream
                        .write_all(final_str.as_bytes())
                        .expect("failed to write to client");
                }

                "ping" => {
                    stream
                        .write_all(b"+PONG\r\n")
                        .expect("failed to write to client");
                }

                "set" => {
                    let mut db = db_instances.write().unwrap();
                    let response = db.set_value(&command);
                    //println!("{:?}", &db.data);
                    stream
                        .write_all(response.to_string().as_bytes())
                        .expect("failed to write to client");
                }

                "get" => {
                    let mut db = db_instances.write().unwrap();
                    match db.get_value(&command) {
                        Some(string_return) => {
                            let parsed_return = command.format_response_code(Some(string_return));
                            stream
                                .write_all(parsed_return.as_bytes())
                                .expect("failed to write to client");
                        }
                        None => {
                            stream
                                .write_all("$-1\r\n".as_bytes())
                                .expect("failed to write to client");
                        }
                    };
                }

                "config" => {
                    if let Some(config_cmd) = command.str_cmd.get(1) {
                        match config_cmd.as_str() {
                            "get" => {
                                let config = config_settings.read().unwrap();
                                let get_string = config.get_config(&command);
                                println!("{:?}", get_string);
                                stream
                                    .write_all(get_string.as_bytes())
                                    .expect("failed to write to client");
                            }

                            "set" => {}

                            _ => {}
                        }
                    }
                }

                "keys" => {
                    let db = db_instances.read().unwrap();
                    if let Some(config_cmd) = command.str_cmd.get(1) {
                        match config_cmd.as_str() {
                            "*" => {
                                let lst_of_keys = match db.get_all_keys() {
                                    Some(vec_str) => vec_str,
                                    None => {
                                        let final_str = format!("*0\r\n");

                                        stream
                                            .write_all(final_str.as_bytes())
                                            .expect("failed to write to client");
                                        return;
                                    }
                                };

                                let base_str = format!("*{}\r\n", lst_of_keys.len());

                                let formatted_item = lst_of_keys
                                    .iter()
                                    .map(|item| format!("${}\r\n{}\r\n", item.len(), item))
                                    .collect::<String>();

                                let final_str = base_str + &formatted_item;

                                stream
                                    .write_all(final_str.as_bytes())
                                    .expect("failed to write to client");
                            }

                            _ => {}
                        }
                    }
                }

                _ => {}
            }
        }

        if reader == 0 {
            break;
        }
    }
}
