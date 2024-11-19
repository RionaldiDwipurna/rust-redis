#![allow(unused_imports)]
use core::panic;
use core::str;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

//#[derive(Debug)]
struct RedisCommand {
    cmd_len: i32,          // Number of commands (e.g., echo hey -> 2)
    n_chars_len: Vec<i32>, // Length of each argument (e.g., [4, 3])
    str_cmd: Vec<String>,  // Command arguments (e.g., ["ECHO", "hey"])
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || match stream {
            Ok(mut stream) => {
                println!("connected");
                event_handler(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}

fn event_handler(stream: &mut TcpStream) {
    let mut buf = [0; 1024];
    loop {
        let reader = stream.read(&mut buf).unwrap();

        let readable = match str::from_utf8(&buf) {
            Ok(v) => v,
            Err(e) => panic!("Invalid utf8 {}", e),
        }
        .trim_end_matches('\0');

        let command = parser_receive(&readable);

        let first_cmd = &command.str_cmd[0];

        //let lst_command = &command.str_cmd;
        //
        //for (i, com) in lst_command.iter().enumerate() {
        //    if i == 0{
        //
        //    }
        //}

        //println!("{:?}", command);
        match first_cmd.as_str() {
            "echo" => {
                let tmp_str = &command.str_cmd[1..];
                let n_len = &command.n_chars_len[1..];
                let final_str = parser_send(tmp_str, n_len);
                //println!("{:?}", final_str);
                stream.write_all(final_str.as_bytes()).unwrap();
            }

            "ping" => {
                stream.write_all(b"+PONG\r\n").unwrap();
            }

            _ => {}
        }

        if reader == 0 {
            break;
        }
    }
}

fn parser_send(str_cmd: &[String], n_chars_len: &[i32]) -> String {
    let mut final_str = String::new();
    for (i, item) in str_cmd.iter().enumerate() {
        final_str.push('$');
        final_str.push_str((n_chars_len[i]).to_string().as_str());
        final_str.push_str("\r\n");
        final_str.push_str(item);
        final_str.push_str("\r\n");
    }
    return final_str;
}

fn parser_receive(command_str: &str) -> RedisCommand {
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

    RedisCommand {
        cmd_len,
        n_chars_len,
        str_cmd,
    }
}

//fn parser(buf: &[u8]) {
//    if buf.is_empty() {
//        return;
//    }
//
//    let mut num: u8;
//    let mut counter = 0;
//
//    loop {
//        num = buf[counter];
//        if num == 0 {
//            break;
//        }
//        println!("{}", &num);
//
//
//        counter += 1;
//    }
//}
