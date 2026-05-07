use rand::Rng;
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ---------------- TASK MODEL ----------------

#[derive(Clone, Debug)]
enum TaskType {
    IO,
    CPU,
}

#[derive(Clone, Debug)]
struct Task {
    id: usize,
    task_type: TaskType,
    created_at: Instant,
}

#[derive(Debug)]
struct TaskResult {
    task_id: usize,
    wait_time: u128,
    turnaround_time: u128,
    task_type: TaskType,
}

// ---------------- SYSTEM STATE ----------------

#[derive(Debug)]
struct SystemState {
    cpu_usage: f64,
    active_workers: usize,
    completed_tasks: usize,
    dispatcher_done: bool,
}

// ---------------- CONSTANTS ----------------

const MAX_CPU: f64 = 100.0;
const IO_CPU: f64 = 10.0;
const CPU_CPU: f64 = 35.0;
const TOTAL_TASKS: usize = 1000;

// ---------------- DISPATCHER ----------------

fn dispatcher(
    queue: Arc<Mutex<VecDeque<Task>>>,
    state: Arc<Mutex<SystemState>>,
    io_ratio: f64,
) {
    let mut rng = rand::thread_rng();

    for i in 0..TOTAL_TASKS {
        let task_type = if rng.gen::<f64>() < io_ratio {
            TaskType::IO
        } else {
            TaskType::CPU
        };

        let task = Task {
            id: i,
            task_type: task_type.clone(),
            created_at: Instant::now(),
        };

        {
            let mut q = queue.lock().unwrap();

            // Prioritize IO tasks
            match task_type {
                TaskType::IO => q.push_front(task),
                TaskType::CPU => q.push_back(task),
            }
        }

        thread::sleep(Duration::from_millis(20));
    }

    let mut s = state.lock().unwrap();
    s.dispatcher_done = true;
}

// ---------------- MANAGER ----------------

fn manager(
    queue: Arc<Mutex<VecDeque<Task>>>,
    worker_tx: mpsc::Sender<Task>,
    state: Arc<Mutex<SystemState>>,
) {
    loop {
        let task = {
            let mut q = queue.lock().unwrap();
            q.pop_front()
        };

        let task = match task {
            Some(t) => t,
            None => {
                let s = state.lock().unwrap();

                if s.dispatcher_done && s.completed_tasks >= TOTAL_TASKS {
                    break;
                }

                drop(s);
                thread::sleep(Duration::from_millis(1));
                continue;
            }
        };

        loop {
            let mut s = state.lock().unwrap();

            let needed_cpu = match task.task_type {
                TaskType::IO => IO_CPU,
                TaskType::CPU => CPU_CPU,
            };

            if s.cpu_usage + needed_cpu <= MAX_CPU && s.active_workers < 8 {
                s.cpu_usage += needed_cpu;
                s.active_workers += 1;

                worker_tx.send(task.clone()).unwrap();
                break;
            }

            drop(s);
            thread::sleep(Duration::from_millis(1));
        }
    }
}

// ---------------- WORKER ----------------

fn worker(
    _id: usize,
    rx: Arc<Mutex<mpsc::Receiver<Task>>>,
    state: Arc<Mutex<SystemState>>,
    result_tx: mpsc::Sender<TaskResult>,
) {
    loop {
        let task = {
            let lock = rx.lock().unwrap();

            match lock.recv() {
                Ok(t) => t,
                Err(_) => break,
            }
        };

        let start_time = Instant::now();

        let cpu_used = match task.task_type {
            TaskType::IO => IO_CPU,
            TaskType::CPU => CPU_CPU,
        };

        let wait_time = start_time
            .duration_since(task.created_at)
            .as_millis();

        // Simulated work
        thread::sleep(Duration::from_millis(200));

        let finish_time = Instant::now();

        let turnaround_time = finish_time
            .duration_since(task.created_at)
            .as_millis();

        result_tx
            .send(TaskResult {
                task_id: task.id,
                wait_time,
                turnaround_time,
                task_type: task.task_type.clone(),
            })
            .unwrap();

        let mut s = state.lock().unwrap();

        s.cpu_usage -= cpu_used;
        s.active_workers -= 1;
        s.completed_tasks += 1;
    }
}

// ---------------- MONITOR ----------------

fn monitor(
    state: Arc<Mutex<SystemState>>,
    start: Instant,
    monitor_tx: mpsc::Sender<(f64, f64, usize)>,
) {
    let mut cpu_sum = 0.0;
    let mut worker_sum = 0.0;
    let mut samples = 0;

    let mut file = File::create("monitor_log.csv").unwrap();

    writeln!(file, "time_ms,cpu_usage,active_workers").unwrap();

    loop {
        {
            let s = state.lock().unwrap();

            let elapsed = start.elapsed().as_millis();

            cpu_sum += s.cpu_usage;
            worker_sum += s.active_workers as f64;
            samples += 1;

            writeln!(
                file,
                "{},{},{}",
                elapsed,
                s.cpu_usage,
                s.active_workers
            )
            .unwrap();

            if s.completed_tasks >= TOTAL_TASKS {
                break;
            }
        }

        thread::sleep(Duration::from_millis(10));
    }

    monitor_tx
        .send((cpu_sum, worker_sum, samples))
        .unwrap();
}

// ---------------- MAIN SIMULATION ----------------

fn run_simulation(io_ratio: f64) {
    // Shared deque queue
    let queue = Arc::new(Mutex::new(VecDeque::<Task>::new()));

    // Channels
    let (tx_worker, rx_worker) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let (monitor_tx, monitor_rx) = mpsc::channel();

    // Shared state
    let state = Arc::new(Mutex::new(SystemState {
        cpu_usage: 0.0,
        active_workers: 0,
        completed_tasks: 0,
        dispatcher_done: false,
    }));

    let start = Instant::now();

    // ---------------- Dispatcher ----------------

    let d_queue = Arc::clone(&queue);
    let d_state = Arc::clone(&state);

    thread::spawn(move || {
        dispatcher(d_queue, d_state, io_ratio)
    });

    // ---------------- Manager ----------------

    let m_queue = Arc::clone(&queue);
    let m_state = Arc::clone(&state);
    let w_tx = tx_worker.clone();

    thread::spawn(move || {
        manager(m_queue, w_tx, m_state)
    });

    // ---------------- Workers ----------------

    let rx_worker = Arc::new(Mutex::new(rx_worker));

    let mut handles = vec![];

    for i in 0..8 {
        let rx = Arc::clone(&rx_worker);
        let st = Arc::clone(&state);
        let rtx = result_tx.clone();

        handles.push(thread::spawn(move || {
            worker(i, rx, st, rtx)
        }));
    }

    // ---------------- Monitor ----------------

    let mon_state = Arc::clone(&state);

    thread::spawn(move || {
        monitor(mon_state, start, monitor_tx)
    });

    // ---------------- Collect Results ----------------

    let mut total_wait = 0.0;
    let mut total_turnaround = 0.0;

    let mut io_wait_total = 0.0;
    let mut cpu_wait_total = 0.0;

    let mut io_count = 0;
    let mut cpu_count = 0;

    let mut max_wait = 0;
    let mut max_wait_task = 0;

    for _ in 0..TOTAL_TASKS {
        if let Ok(res) = result_rx.recv() {
            total_wait += res.wait_time as f64;
            total_turnaround += res.turnaround_time as f64;

            if res.wait_time > max_wait {
                max_wait = res.wait_time;
                max_wait_task = res.task_id;
            }

            match res.task_type {
                TaskType::IO => {
                    io_wait_total += res.wait_time as f64;
                    io_count += 1;
                }

                TaskType::CPU => {
                    cpu_wait_total += res.wait_time as f64;
                    cpu_count += 1;
                }
            }
        }
    }

    // ---------------- Monitor Results ----------------

    let (cpu_sum, worker_sum, samples) =
        monitor_rx.recv().unwrap();

    let avg_wait = total_wait / TOTAL_TASKS as f64;
    let avg_turnaround =
        total_turnaround / TOTAL_TASKS as f64;

    let avg_io_wait =
        if io_count > 0 {
            io_wait_total / io_count as f64
        } else {
            0.0
        };

    let avg_cpu_wait =
        if cpu_count > 0 {
            cpu_wait_total / cpu_count as f64
        } else {
            0.0
        };

    let avg_cpu = cpu_sum / samples as f64;
    let avg_workers = worker_sum / samples as f64;

    let total_runtime = start.elapsed().as_millis();
    let makespan = total_runtime;

    // Shutdown
    drop(tx_worker);
    drop(result_tx);

    for h in handles {
        h.join().unwrap();
    }

    // ---------------- OUTPUT ----------------

    println!("== Optimized simulation ==");

    println!(
        "{} tasks, {:.0}% IO / {:.0}% CPU, 8 workers, cap 100%",
        TOTAL_TASKS,
        io_ratio * 100.0,
        (1.0 - io_ratio) * 100.0
    );

    println!("\n— results —");

    println!(
        "total runtime      : {} ms",
        total_runtime
    );

    println!(
        "makespan           : {} ms",
        makespan
    );

    println!(
        "tasks completed    : {} (IO={}, CPU={})",
        TOTAL_TASKS,
        io_count,
        cpu_count
    );

    println!(
        "avg wait time      : {:.2} ms",
        avg_wait
    );

    println!(
        "avg wait (IO only) : {:.2} ms",
        avg_io_wait
    );

    println!(
        "avg wait (CPU only): {:.2} ms",
        avg_cpu_wait
    );

    println!(
        "avg turnaround time: {:.2} ms",
        avg_turnaround
    );

    println!(
        "max wait time      : {} ms (task #{})",
        max_wait,
        max_wait_task
    );

    println!(
        "avg CPU usage      : {:.2} %",
        avg_cpu
    );

    println!(
        "avg workers active : {:.2} / 8",
        avg_workers
    );

    println!(
        "monitor samples    : {}",
        samples
    );

    println!(
        "monitor csv        : monitor_log.csv"
    );
}

// ---------------- MAIN ----------------

fn main() {
    run_simulation(0.7); // 70% IO / 30% CPU
}