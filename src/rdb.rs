use crate::redis_config;
use crate::redis_config::RedisConfig;
use crate::RedisCommand;
use core::panic;
use core::str;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::read;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::u64;

pub enum RedisResponse {
    OK(String),
    Error(String),
}

impl RedisResponse {
    pub fn to_string(&self) -> String {
        match self {
            RedisResponse::OK(message) => format!("+{}\r\n", message),
            RedisResponse::Error(message) => format!("-{}\r\n", message),
        }
    }
}

pub enum ReplicationRole {
    Master,
    Slave,
}

pub struct RedisData {
    pub data: HashMap<String, String>,
    pub expiry: HashMap<String, std::time::SystemTime>,

    pub replication_role: ReplicationRole,
    pub host: Option<String>,
    pub port: Option<u16>,
}

impl RedisData {
    pub fn init_db(role: ReplicationRole, host: Option<String>, port: Option<u16>) -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
            replication_role: role,
            host: host,
            port: port,
        }
    }

    pub fn decode_length(&self, content: &Vec<u8>, cursor: &mut usize) -> Option<usize> {
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

    pub fn read_values(&self, content: &Vec<u8>, cursor: &mut usize) -> Option<(String, String)> {
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

    pub fn read_string(&self, content: &Vec<u8>, cursor: &mut usize) -> Option<String> {
        let length = self.decode_length(content, cursor)?;
        if *cursor + length <= content.len() {
            let string_data = &content[*cursor..*cursor + length];
            *cursor += length;
            Some(String::from_utf8_lossy(string_data).to_string())
        } else {
            None
        }
    }

    pub fn parse_db_key_val(&mut self, content: &Vec<u8>, cursor: &mut usize) {
        *cursor += 1;
        let size_table_all = content[*cursor];
        println!("Table size all: {}", size_table_all);

        *cursor += 1;
        let size_table_expired = content[*cursor];
        println!("Table size expired: {}", size_table_expired);

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

    pub fn read_from_file(&mut self, rconfig: &RedisConfig) -> RedisResponse {
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

    pub fn set_value(&mut self, command: &RedisCommand) -> RedisResponse {
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

    pub fn get_value(&mut self, command: &RedisCommand) -> Option<String> {
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

    pub fn get_all_keys(&self) -> Option<Vec<String>> {
        let keys = self.data.clone().into_keys().collect::<Vec<String>>();
        if keys.len() == 0 {
            None
        } else {
            Some(keys)
        }
    }

    pub fn get_role(&self) -> &ReplicationRole {
        &self.replication_role
    }

    pub fn get_host_port(&self) -> (Option<String>, Option<u16>) {
        (self.host.clone(), self.port.clone())
    }
}
