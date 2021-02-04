use std::env;

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::Duration;

use std::thread::{self, sleep};
use std::sync::mpsc::{Receiver, Sender, channel};

use io::Read;
use postgres::{Client, NoTls};
use mysql::{Pool, PooledConn, prelude::Queryable};

static WAIT_READ: Duration = Duration::from_millis(10);
static WAIT_CONNECT: Duration = Duration::from_secs(3);

trait DbConn {
    fn execute(&mut self, query: &str) -> bool;
    fn is_alive(&mut self) -> bool;
}

impl DbConn for () {
    fn execute(&mut self, _query: &str) -> bool {
        false
    }
    fn is_alive(&mut self) -> bool {
        false
    }
}

impl DbConn for Client {
    fn execute(&mut self, query: &str) -> bool {
        self.batch_execute(query).is_ok()
    }
    fn is_alive(&mut self) -> bool {
        self.is_closed()
    }
}

impl DbConn for PooledConn {
    fn execute(&mut self, query: &str) -> bool {
        self.query_drop(query).is_ok()
    }
    fn is_alive(&mut self) -> bool {
        true
    }
}

fn get_conn(url: &str) -> Result<Box<dyn DbConn>, (bool, String)> {

    if url.starts_with("postgresql") {
        match Client::connect(url, NoTls) {
            Ok(conn) => return Ok(Box::new(conn)),
            Err(e) => return Err((false, e.to_string())),
        }
    }
    else if url.starts_with("mysql") {
        match Pool::new(url) {
            Ok(pool) => {
                match pool.get_conn() {
                    Ok(conn) => return Ok(Box::new(conn)),
                    Err(e) => return Err((false, e.to_string())),
                }        
            }
            Err(e) => return Err((false, e.to_string())),
        }
    }

    Err((true, "no database!".to_string()))
}

fn write_db(rx: Receiver<String>, url: &str) {

    let mut db: Box<dyn DbConn> = Box::new(());

    loop {
        if !db.is_alive() {
            match get_conn(url) {
                Ok(conn) => db = conn,
                Err((true, _)) => return,
                Err((false, e)) => {
                    println!("{}", e);
                    sleep(WAIT_CONNECT);
                    continue;
                },
            }
        }

        match rx.recv() {
            Ok(query) => {
                let res = db.execute(&query);
                println!("written({}) - {}", res, &query);        
            },
            Err(_) => {
                println!("closing write!");
                return;
            }
        }
    }
}

fn read_fifo(tx: Sender<String>, path: &str) {

    let full_path = Path::new(&path);

    let file = File::open(&full_path).expect("could not open file");
    let mut buf = io::BufReader::new(file);

    loop {
        sleep(WAIT_READ);

        for query in buf.by_ref().lines() {
            if !tx.send(query.unwrap()).is_ok() {
                println!("closing read!");
                return;
            }
        }
    }
}

fn main() {

    if env::args().count() < 3 {
        panic!("no arguments");
    }

    let fifo_path = env::args().nth(2).unwrap();
    let db_url = env::args().nth(1).unwrap();

    println!("{} {}", db_url, fifo_path);

    let (tx, rx) = channel::<String>();

    let handle = thread::spawn(move || {
        write_db(rx, &db_url);
    });

    read_fifo(tx, &fifo_path);

    handle.join().unwrap();
}