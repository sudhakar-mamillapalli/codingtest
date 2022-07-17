extern crate csv;
extern crate serde;
extern crate num_cpus;

use csv::{ReaderBuilder, Trim};

use std::env;
//use std::error::Error;

use std::fs::{File};
use std::io::{BufReader};

use std::io;
use std::io::prelude::*;

use std::thread;
use std::sync::mpsc;

use std::collections::HashMap;


use serde::{Deserialize};


#[derive(Debug, Deserialize)]
struct InputRecord {
     #[serde(rename = "type")]
    tx_type: String,
     #[serde(rename = "client")]
    tx_client: u16,
     #[serde(rename = "tx")]
    tx_id: u32,
    #[serde(rename = "amount")]
    tx_amount: Option<f32>,
}

// Structure used to keep track of worker threads being spawned
struct ThreadInfo {
    tx: mpsc::Sender<InputRecord>,
    handle: thread::JoinHandle<u32>,
}

fn main() {


    let mut args = env::args();
    let input_file = args.nth(1).expect("Please give input file name");


    /* Based on no of cpus create worker threads */
    let mut thread_count = num_cpus::get();
    thread_count &= !0x2; // make it power of 2, just in case

    // create a stream of input records, using serde/csv
    let file = File::open(input_file).expect("Failed to open input file");
    let buf_reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_reader(buf_reader);

    // now use worker threads to process the input records
    start_work(thread_count, &mut rdr);
}


fn start_work(thread_count: usize, rdr: &mut csv::Reader<BufReader<File>>) -> u32 {

    /* Start thread_count worker threads and give them each the rx end
     * of a channel where they will receive input records to process.
     *
     * The main thread reads in input record and based on clients id
     * distributes the work to one of the worker threads.  Note that
     * mapping from a client to a worker thread is fixed.
     */
    let mut thread_info_vec = Vec::new();
    for _ in 0..thread_count {
        let (tx, rx) = mpsc::channel();
        let handle  = thread::spawn( || worker_func(rx) );
        let thread_info = ThreadInfo{tx: tx, handle: handle};
        thread_info_vec.push(thread_info);
    }

    /*
     * Using Serde/CSV read the input data as a stream  and based on
     * client id send to one of the above worker threads. Workers process
     * input and write it out to stdout
     */
    println!("client, available, held, total, locked");
    io::stdout().flush().expect("Could not flush stdout");
    for result in rdr.deserialize() {
        let input_record: InputRecord = result.expect("Failed to parse input record");
        let tx_client = input_record.tx_client;
        let thread_info = &thread_info_vec[(tx_client as usize)%thread_count];
        thread_info.tx.send(input_record).unwrap();
    }

    // Now close all channels and join with worker threads
    let mut count = 0;
    for info in thread_info_vec {
        drop(info.tx);
        count += info.handle.join().expect("failed to join with worker thread");
    }
    // Return number of client record
    count
}

#[derive(Debug)]
struct TxRecord {
    record: InputRecord,
    in_dispute: bool,
}

// Keep the processed information regarding a client in here. Note we
// keep list of transactions of this client here, since if there is
// a dispute then need to go back and get the original transaction
// information.
#[derive(Debug)]
struct ClientAccount {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    txs: HashMap<u32, TxRecord>,
    locked: bool,
}

// Worker func.  Processes and print outs the account information for
// client it is resposible for to stdout
fn worker_func(rx: mpsc::Receiver<InputRecord>) -> u32 {
    let mut count = 0;
    for client_acct in  worker_func_helper(rx) {
        count += 1;
        println!("{}, {:.4}, {:.4}, {:.4}, {}", client_acct.client, client_acct.available, client_acct.held, client_acct.total, client_acct.locked);
    }
    count
}

// Business logic for worker is here.  Returns iterator of processed client
// records
fn worker_func_helper (rx: mpsc::Receiver<InputRecord>) -> impl Iterator<Item = ClientAccount> {
    let mut client_acct_map = HashMap::new();
    for record in rx {
        let client_acct = client_acct_map.entry(record.tx_client).
            or_insert(ClientAccount{
                client: record.tx_client,
                available: 0.0, held: 0.0, total: 0.0,
                txs: HashMap::new(),
                locked: false});
        if client_acct.locked {
            continue;
        }
        if record.tx_type.eq_ignore_ascii_case("deposit") {
            let tx_amount = record.tx_amount.unwrap();
            client_acct.available += tx_amount;
            client_acct.total += tx_amount;
            client_acct.txs.insert(record.tx_id,
                                   TxRecord{record: record, in_dispute: false});

        } else if record.tx_type.eq_ignore_ascii_case("withdrawl") || record.tx_type.eq_ignore_ascii_case("withdrawal") {
            // seems long wrong spelling in the question. Accept both

            let tx_amount = record.tx_amount.unwrap();
            if  tx_amount <= client_acct.available {
                client_acct.available -= tx_amount;
                client_acct.total -= tx_amount;
                client_acct.txs.insert(record.tx_id,
                                       TxRecord{record: record, in_dispute: false});
            }
        } else if record.tx_type.eq_ignore_ascii_case("dispute") {

            if let Some(transaction) = client_acct.txs.get_mut(&record.tx_id) {
                let tx_amount = transaction.record.tx_amount.unwrap();
                client_acct.available -= tx_amount;
                client_acct.held += tx_amount;
                transaction.in_dispute = true;

            }
        } else if record.tx_type.eq_ignore_ascii_case("resolve") {

            if let Some(transaction) = client_acct.txs.get_mut(&record.tx_id) {
                if transaction.in_dispute == true {
                    let tx_amount = transaction.record.tx_amount.unwrap();
                    client_acct.available += tx_amount;
                    client_acct.held -= tx_amount;
                    transaction.in_dispute = false;
                }
            }
        } else if record.tx_type.eq_ignore_ascii_case("chargeback") {

            if let Some(transaction) = client_acct.txs.get(&record.tx_id) {
                if transaction.in_dispute == true {
                    let tx_amount = transaction.record.tx_amount.unwrap();
                    client_acct.held -= tx_amount;
                    client_acct.total -= tx_amount;
                    client_acct.locked = true;
                }
            }
        } else {

            panic!("Unexpected record type: {:?}", record);
        }
    }
    // return a iterator of client accts.  The caller can either print
    // it or tester can test if they match results
    client_acct_map.into_values()
}

#[cfg(test)]
mod test {
    use super::*;

    // The end result for each client account is represented as this
    // tuple below, so taht we can compare to what we expect.  It
    // is basically (client_id, avaialble, held, total and locked)
    type Result = (u16, f32, f32, f32, bool);

    fn test_helper_func(data: &'static str, expected_output: &[Result]) {
        let (tx, rx) = mpsc::channel();
        let _handle = thread::spawn(move || {
            let mut rdr = ReaderBuilder::new()
                .trim(Trim::All)
                .from_reader(data.as_bytes());
            for result in rdr.deserialize() {
                let input_record: InputRecord = result.expect("Failed to parse");
                tx.send(input_record).expect("failed to send record");
            }
            drop(tx);
        });
        for client_acct in  worker_func_helper(rx) {
            let output = (client_acct.client, client_acct.available, client_acct.held, client_acct.total, client_acct.locked);
            //println!("{:?}", output);
            assert!(expected_output.contains(&output));
        }
    }

    fn test_helper_func2(data: &'static str, expected_count: &[u32]) {
        // for multi-threaded processing.  Just use two threads for testing
        let mut rdr = ReaderBuilder::new()
            .trim(Trim::All)
            .from_reader(data.as_bytes());

        // Doing some part for start_work here.  Maybe could do it a better
        // way
        let (tx1, rx1) = mpsc::channel();
        let handle1  = thread::spawn( || worker_func(rx1) );
        let (tx2, rx2) = mpsc::channel();
        let handle2  = thread::spawn( || worker_func(rx2) );
        for result in rdr.deserialize() {
            let input_record: InputRecord = result.expect("Failed to parse input record");
            let tx_client = input_record.tx_client;
            if (tx_client as usize)%2 == 0 {
                tx1.send(input_record).unwrap();
            } else {
                tx2.send(input_record).unwrap();
            }
        }
        drop(tx1);
        drop(tx2);

        let count1 = handle1.join().expect("failed to join with worker thread");
        let count2 = handle2.join().expect("failed to join with worker thread");

        // just make sure each thread processed expected number of records
        assert_eq!(count1, expected_count[0]);
        assert_eq!(count2, expected_count[1]);
 }


    #[test]
    fn record_read_test() {
           let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 1, 3, 2.0
           withdrawal, 1, 4, 1.5
           dispute, 1, 3,
           resolve, 1, 3,
           chargeback, 1, 3,
           withdrawal, 2, 5, 3.0";
           let mut rdr = ReaderBuilder::new()
               .trim(Trim::All)
               .from_reader(data.as_bytes());
           let mut count = 0;
           for result in rdr.deserialize() {
               let _input_record: InputRecord = result.expect("Failed to parse");
               count += 1;
           }
           assert_eq!(8, count)
    }

    #[test]
    fn basic_test() {
        let data = "\
           type, client, tx, amount
           deposit,    1, 1, 1.0
           deposit,    2, 2, 2.0
           deposit,    1, 3, 2.0
           withdrawal, 1, 4, 1.5
           withdrawl,  2, 5, 2.0
           deposit,    2, 6, 2.1234";
        let expected_output = [(1, 1.5, 0.0, 1.5, false), (2, 2.1234, 0.0, 2.1234, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn bad_withdrawl_test() {
        // withdraw more than available
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 1, 3, 2.0
           withdrawal, 1, 4, 1.5
           withdrawl, 2, 5, 10.0";
        let expected_output = [(1, 1.5, 0.0, 1.5, false), (2, 2.0, 0.0, 2.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    #[should_panic]
    fn bad_data_test() {
        // supply bad data and program should panic
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 1, 3, 2.0
           with, 1, 4, 1.5
           withdrawl, 2, 5, 3.0";
        let expected_output = [(1, 1.5, 0.0, 1.5, false), (2, 2.0, 0.0, 2.0, false)];
        test_helper_func(data, &expected_output);
    }


    #[test]
    fn dispute_test() {
        // test dispute
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 3,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 2.0, 2.0, 4.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn resolve_test() {
        // test resolution of disputed transaction
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 3,
           resolve, 2, 3,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 4.0, 0.0, 4.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn chargeback_test() {
        // test chargeback of disputed transaction
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 3,
           chargeback, 2, 3,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 2.0, 0.0, 2.0, true)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn nonexistent_dispute_test() {
        // test dispute of non-existent transaction. Should ignore
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 4,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 4.0, 0.0, 4.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn nonexistent_resolve_test() {
        // test resolution of non-existent transaction. Should ignore
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 3,
           resolve, 2, 4,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 2.0, 2.0, 4.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn nonexistent_chargeback_test() {
        // test chargeback of non-existent transaction. Should ignore
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 3,
           chargeback, 2, 4,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 2.0, 2.0, 4.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn resolve_without_dispute_test() {
        // test resoluiton of transaction which hasn't been disputed. Should ignore
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           resolve, 2, 4,";
        let expected_output = [(1, 1.0, 0.0, 1.0, false), (2, 4.0, 0.0, 4.0, false)];
        test_helper_func(data, &expected_output);
    }

    #[test]
    fn multi_thread_test() {
        // processing by two threads equally
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 2, 2, 2.0
           deposit, 2, 3, 2.0
           dispute, 2, 3,
           resolve, 2, 3,";
        let expected_count = [1, 1];
        test_helper_func2(data, &expected_count);
    }

    #[test]
    fn multi_thread_test_2() {
        // processing only by one thread
        let data = "\
           type, client, tx, amount
           deposit, 1, 1, 1.0
           deposit, 3, 2, 2.0
           deposit, 3, 3, 2.0
           dispute, 3, 3,
           resolve, 3, 3,";
        let expected_count = [0, 2];
        test_helper_func2(data, &expected_count);
    }
}

