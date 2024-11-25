#![allow(unused_imports)]
use core::panic;
use core::str;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::{Arc, MutexGuard, RwLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;

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
                        config.insert(arg.clone(), args_iter.next().unwrap().clone());
                    }

                    None => {
                        panic!("Missing value for the flag");
                    }
                }
            }
        }
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
            .enumerate()
            .map(|(i, item)| format!("${}\r\n{}\r\n", item.len(), item))
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
        let mut cmd_len = 0;
        let mut n_chars_len = Vec::new();
        let mut str_cmd = Vec::new();

        while let Some(line) = lines.next() {
            match line.chars().next() {
                Some('*') => {
                    cmd_len = line
                        .trim_start_matches('*')
                        .parse::<i32>()
                        .expect("Failed to parse command length");
                }

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
    expiry: HashMap<String, std::time::Instant>,
}

impl RedisData {
    fn init_db() -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    fn set_value(&mut self, command: &RedisCommand) -> RedisResponse {
        self.data
            .insert(command.str_cmd[1].clone(), command.str_cmd[2].clone());
        if command.cmd_len == 5 && command.str_cmd[3].as_str() == "px" {
            self.expiry.insert(
                command.str_cmd[1].clone(),
                Instant::now()
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
            if Instant::now() > expiry_time {
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
}

fn main() {
    //env::set_var("RUST_BACKTRACE", "full");
    let args: Vec<String> = env::args().collect();
    let config_struct = RedisConfig::parse_argument(args);
    //println!("{:?}", config_struct.config);
    let config_settings = Arc::new(RwLock::new(config_struct));

    println!("Logs from your program will appear here!");

    let mut handles = vec![];

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let redis_data = RedisData::init_db();
    let db_instances = Arc::new(RwLock::new(redis_data));

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

                _ => {}
            }
        }

        if reader == 0 {
            break;
        }
    }
}
