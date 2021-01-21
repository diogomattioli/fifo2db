use std::env;

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use std::thread;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use mysql::{Pool, PooledConn};
use mysql::prelude::Queryable;

use postgres::{Client, NoTls};

static WAIT_READ_MS: Duration = Duration::from_millis(10);
static WAIT_WRITE_MS: Duration = Duration::from_millis(100);

trait DbConn {
    fn execute(&mut self, query: &String) -> bool;
}

impl DbConn for Client {
    fn execute(&mut self, query: &String) -> bool {
        self.batch_execute(query).is_ok()
    }
}

impl DbConn for PooledConn {
    fn execute(&mut self, query: &String) -> bool {
        self.query_drop(query).is_ok()
    }
}

fn write_db(arc: Arc<Mutex<VecDeque<String>>>, url: &String) {

    let mut db: Box<dyn DbConn>;

    if url.starts_with("postgresql") {
        db = Box::new(Client::connect(url, NoTls).unwrap());
    }
    else if url.starts_with("mysql") {
        let pool = Pool::new(url).unwrap();
        db = Box::new(pool.get_conn().unwrap());
    }
    else {
        panic!("no database");
    }

    loop {
        let query;
        {
            let mut queries = arc.lock().unwrap();
            query = queries.pop_front();
        }

        if query.is_some() {
            let query_str = query.unwrap();
            let res = db.execute(&query_str);
            println!("written({}) - {}", res, query_str);
        }
        else {
            sleep(WAIT_WRITE_MS);
        }
    }

}

fn read_fifo(arc: Arc<Mutex<VecDeque<String>>>, path: &String) {

    let full_path = Path::new(&path);

    let file = match File::open(&full_path) {
        Err(reason) => panic!("could not open {}: {}", full_path.display(), reason),
        Ok(file) => file,
    };

    let mut buf = io::BufReader::new(file);
    loop {
        let mut query = String::new();
        let size = buf.read_line(&mut query).unwrap();
        if size > 0 {
            query.remove(query.len() - 1);
            println!("read    - {}", query);
            let mut queries = arc.lock().unwrap();
            queries.push_back(query);
        }
        else {
            sleep(WAIT_READ_MS);
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

    let queries: VecDeque<String> = VecDeque::new();
    let mutex = std::sync::Mutex::new(queries);
    let arc = std::sync::Arc::new(mutex);

    let mut handles = VecDeque::new();
    {
        let arc = arc.clone();
        let handle = thread::spawn(move || {
            write_db(arc, &db_url);
        });
        handles.push_back(handle);
    }

    read_fifo(arc, &fifo_path);

    for handle in handles {
        handle.join().unwrap();
    }
}
