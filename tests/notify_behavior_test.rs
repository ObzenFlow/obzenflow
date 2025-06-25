use std::sync::Arc;
use tokio::sync::Notify;

#[tokio::test]
async fn test_notify_race_condition() {
    println!("Testing Notify behavior...");
    
    let signal = Arc::new(Notify::new());
    
    // Test 1: Fire before listener
    println!("\nTest 1: Fire before listener");
    signal.notify_waiters();
    
    let notified = signal.notified();
    println!("Created listener after firing");
    
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(100),
        notified
    ).await;
    
    match result {
        Ok(_) => println!("Listener received signal!"),
        Err(_) => println!("Listener timed out (expected)")
    }
    
    // Test 2: Listener before fire
    println!("\nTest 2: Listener before fire");
    let signal2 = Arc::new(Notify::new());
    let notified2 = signal2.notified();
    println!("Created listener before firing");
    
    signal2.notify_waiters();
    println!("Fired signal");
    
    let result2 = tokio::time::timeout(
        tokio::time::Duration::from_millis(100),
        notified2
    ).await;
    
    match result2 {
        Ok(_) => println!("Listener received signal!"),
        Err(_) => println!("Listener timed out")
    }
    
    // Test 3: Prepare listener, spawn task, then fire
    println!("\nTest 3: Prepare listener, spawn task, then fire");
    let signal3 = Arc::new(Notify::new());
    let signal3_task = signal3.clone();
    
    let handle = tokio::spawn(async move {
        let notified3 = signal3_task.notified();
        println!("Task: waiting for signal...");
        notified3.await;
        println!("Task: received signal!");
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    println!("Main: firing signal");
    signal3.notify_waiters();
    
    let _ = handle.await;
}