use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

// Define a type for our processing function
type ProcessingFn = fn(usize, i32) -> i32;

//Define the cpu resource
//static mut CPU_RESOURCE: i16 = 0;


// Define a function that processes data by squaring it
fn cpu_task(id: usize){
    //Need to make a mutex
    println!("Worker {} is performing a cpu task", id);
    //CPU_RESOURCE += 35;
    thread::sleep(Duration::from_millis(200));

    println!("Worker {} finished cpu task.", id);
    
}

// Define another processing function that doubles data
fn io_task(id: usize){
    
    println!("Worker {} performing io task", id);
    thread::sleep(Duration::from_millis(200));
    //CPU_RESOURCE += 10;

    println!("Worker {} finished io task", id);
}

// This function creates worker threads and takes a processing function as a parameter
fn create_worker_pool(
    num_workers: usize,
    processor: ProcessingFn,
) -> (
    Vec<thread::JoinHandle<()>>,
    mpsc::Sender<i32>,
    mpsc::Receiver<i32>,
) {
    // Create channels for communication
    let (task_tx, task_rx) = mpsc::channel(); // For sending tasks to workers
    let (result_tx, result_rx) = mpsc::channel(); // For receiving results

    // Wrap the task receiver in Arc<Mutex> to share among workers
    let task_rx = Arc::new(Mutex::new(task_rx));

    // Create worker threads
    let mut handles = vec![];

    for worker_id in 1..=num_workers {
        let task_rx_clone = Arc::clone(&task_rx);
        let result_tx_clone = result_tx.clone();

        let handle = thread::spawn(move || loop {
            // Limit the scope of the lock so it is released immediately
            let value = {
                let receiver = task_rx_clone.lock().unwrap();
                receiver.recv()
            };

            let value = match value {
                Ok(val) => val,
                Err(_) => break, // Channel closed
            };

            if value == -1 {
                println!("Worker {} received termination signal", worker_id);
                break;
            }

            // Call the processing function
            let result = processor(worker_id, value);

            // Send result back to main thread
            if result_tx_clone.send(result).is_err() {
                break;
            }
        });

        handles.push(handle);
    }

    (handles, task_tx, result_rx)
}

fn main() {
    println!("=== Starting worker pool with squaring function ===");

    let (handles, task_tx, results) = create_worker_pool(8, cpu_task);

    for i in 1..=10 {
        task_tx.send(i).unwrap();
        println!("Main: Sent value {} for processing", i);
    }

    // Send termination signal
    for _ in 0..3 {
        task_tx.send(-1).unwrap();
    }

    drop(task_tx);

    let mut result_count = 0;
    while result_count < 10 {
        match results.recv() {
            Ok(result) => {
                println!("Main: Received result: {}", result);
                result_count += 1;
            }
            Err(_) => break,
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("\n=== Starting worker pool with doubling function ===");

    let (handles, task_tx, results) = create_worker_pool(2, fn(usize) -> () {io_task});

    for i in 1..=10 {
        task_tx.send(i).unwrap();
        println!("Main: Sent value {} for processing", i);
    }

    for _ in 0..2 {
        task_tx.send(-1).unwrap();
    }

    drop(task_tx);

    let mut result_count = 0;
    while result_count < 10 {
        match results.recv() {
            Ok(result) => {
                println!("Main: Received result: {}", result);
                result_count += 1;
            }
            Err(_) => break,
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("All workers have completed their tasks");
}