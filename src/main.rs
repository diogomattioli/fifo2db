use std::{env, sync::{Arc, Mutex}, thread::JoinHandle};

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::Duration;

use std::thread::{self, sleep};
use std::sync::mpsc::{Receiver, Sender, channel};

use diesel::{RunQueryDsl, sql_query};
use io::Read;
use r2d2::Pool;
use r2d2_diesel::ConnectionManager;

static WAIT_READ: Duration = Duration::from_millis(10);
static WAIT_CONNECT: Duration = Duration::from_secs(3);

pub type DbConnection = diesel::pg::PgConnection;

fn write_db(rx: Arc<Mutex<Receiver<String>>>, pool: Pool<ConnectionManager<DbConnection>>) {

    loop {
        let received = rx.lock().unwrap().recv();

        if let Ok(query) = received {
            loop {
                if let Ok(conn) = pool.get() {
                    let res = sql_query(&query).execute(&(*conn));
                    println!("written({}) - {}", res.is_ok(), &query);       
                    break;
                }

                sleep(WAIT_CONNECT);
            }
        }
        else {
            println!("closing write!");
            return;
        }
    }
}

fn read_fifo(tx: Sender<String>, path: &str) {

    let full_path = Path::new(&path);

    let file = File::open(&full_path).expect("could not open file");
    let mut buf = io::BufReader::new(file);

    loop {
        for query in buf.by_ref().lines() {
            if !tx.send(query.unwrap()).is_ok() {
                println!("closing read!");
                return;
            }
        }

        sleep(WAIT_READ);
    }
}

fn main() {

    if env::args().count() < 3 {
        panic!("usage: {} fifo_path pool_url [pool_size]", env::args().nth(0).unwrap());
    }

    let fifo_path = env::args().nth(1).unwrap();
    let pool_url = env::args().nth(2).unwrap();
    let mut pool_size = 3;

    if let Some(str) = env::args().nth(3) {
        if let Ok(v) = u8::from_str_radix(&str, 10) {
            if v > 0 {
                pool_size = v;
            }
        }
    }

    println!("{} {}", pool_url, fifo_path);

    let manager = ConnectionManager::<DbConnection>::new(pool_url);
    let pool = Pool::builder()
        .max_size(pool_size as u32)
        .build(manager)
        .expect("failed to create pool");

    let (tx, rx) = channel::<String>();
    let arc_rx = Arc::new(Mutex::new(rx));
    
    let mut handles: Vec<JoinHandle<()>> = vec![];
    for _ in 1..=pool_size {
        let rx = Arc::clone(&arc_rx);
        let pool = pool.clone();
        let handle = thread::spawn(move || {
            write_db(rx, pool);
        });
        handles.push(handle);
    }

    read_fifo(tx, &fifo_path);

    for handle in handles {
        handle.join().unwrap();
    }
}