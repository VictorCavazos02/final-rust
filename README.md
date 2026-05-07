# final-rust

This is my final rust project!
To execute, just run {cargo run} in the terminal when in the /final_project directory

The optimized version is in the pushes labled [Deque Version], so you'll have to go through there in order to execute it.

The rust program [Task Dispatcher] is a multithreading task scheduler that simulates how an operating system could possibly manage CPU and I/O workloads. The purpose of the program is to create 1000 tasks (with 70% being IO tasks & 30% being CPU tasks) and manage them through the First In First Out scheduling order. The program monitors the CPU usage, worker thread activity, and turnaround time for each task as it comes and goes. The program uses multiple libraries from Rust’s toolbox such as threads, channels, mutexes, and shared memory structures to complete the program efficiently.

The first experiment FIFIO Version is slower and unoptimized, but the Deque Version takes up almost half the time as FIFO! 