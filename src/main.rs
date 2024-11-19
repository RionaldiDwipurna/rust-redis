#![allow(unused_imports)]
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

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
        //println!("reading: {}", reader);
        if reader == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}
