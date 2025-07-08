use std::time::{SystemTime, UNIX_EPOCH, Duration};

fn main() {
    // Test std::thread::sleep timing
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    std::thread::sleep(Duration::from_millis(5));
    
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let duration_ms = (end - start) / 1_000_000;
    
    println!("Sleep duration: {}ms", duration_ms);
    println!("Start: {} nanos", start);
    println!("End: {} nanos", end);
    println!("Diff: {} nanos", end - start);
}