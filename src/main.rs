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
mod rdb;
mod redis_config;
use rdb::RedisData;
use rdb::RedisResponse;
use redis_config::RedisConfig;

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

fn main() {
    //env::set_var("RUST_BACKTRACE", "full");
    let args: Vec<String> = env::args().collect();
    let config_struct = RedisConfig::parse_argument(args);
    let mut redis_data = RedisData::init_db();

    let read_file = redis_data.read_from_file(&config_struct);

    println!("Logs from your program will appear here!");

    let mut handles = vec![];

    let port = match config_struct.get_port() {
        Some(port) => port.clone(),
        None => "6379".to_string(),
    };

    let address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(address).unwrap();

    let config_settings = Arc::new(RwLock::new(config_struct));
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
