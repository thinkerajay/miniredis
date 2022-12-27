use std::collections::HashMap;
use std::ptr::write;
use std::sync::{Arc, Mutex};

use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> io::Result<()> {
    let in_mem_db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    let redis_server = TcpListener::bind("127.0.0.1:9985").await;
    match redis_server {
        Ok(server) => {
            println!("Running server at 127.0.0.1:9985 ...");
            loop {
                match server.accept().await {
                    Ok((connection, addr)) => {
                        println!("Received connection :{:?}", addr.to_string());
                        let in_mem_db_clone = in_mem_db.clone();
                        tokio::spawn(async move {
                            process(connection, in_mem_db_clone).await;
                        });
                    }
                    _ => break
                }
            }
        }
        Err(e) => {
            eprintln!("{}", e);
        }
    }
    Ok(())
}


async fn process(mut connection: TcpStream, in_mem_db: Arc<Mutex<HashMap<String, String>>>) {
    let mut buf = String::with_capacity(20);
    let (mut reader, mut writer) = tokio::io::split(connection);
    let mut buf_reader = BufReader::new(reader);
    loop {
        let mut n_bytes = buf_reader.read_line(&mut buf).await;
        println!("inserted value {:?}", in_mem_db.lock().unwrap().len());
        match n_bytes {
            Ok(n) if n > 0 => {
                let req = buf.to_string();
                let request: Vec<&str> = req.splitn(3, " ").collect();
                match request[0].to_ascii_uppercase().as_ref() {
                    "SET" => {
                        println!("received SET command with key: {} value: {}", request[1], request[2]);
                        let key = request[1].to_string();
                        let mut value = request[2].to_string();
                        value.pop();
                        in_mem_db.lock().unwrap().insert(key, value);
                    }
                    "GET" => {
                        let mut key = request[1].to_string();
                        key.pop();
                        println!("received GET command with key: {:?}", key);
                        let mut vals: String = String::new();
                        match in_mem_db.lock().unwrap().get(&key).as_ref() {
                            Some(val) => {
                                println!("got value: {}", val);
                                vals = val.to_string();
                            }
                            None => {
                                println!("no value");
                            }
                        }
                        writer.write(vals.as_bytes()).await.expect("valid value");
                    }
                    _ => {}
                }
                buf.clear();
            }
            Ok(n) if n <= 0 => {
                println!("reached EOF");
                return;
            }
            Err(_) => {
                println!("reached EOF");
                return;
            }
            _ => {}
        }
    }
}
