use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
const REGISTRY_PATH: &str = "/dev/shm/hyli_test_ports";
#[cfg(not(target_os = "linux"))]
const REGISTRY_PATH: &str = "/tmp/hyli_test_ports";

pub fn find_available_port_sync() -> u16 {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(REGISTRY_PATH)
        .expect("Failed to open port registry");

    // Exclusive cross-process lock
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };

    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();

    // Filter out entries from dead processes
    let live_ports: std::collections::HashSet<u16> = content
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid: u32 = parts.next()?.parse().ok()?;
            let port: u16 = parts.next()?.parse().ok()?;
            if unsafe { libc::kill(pid as libc::pid_t, 0) == 0 } {
                Some(port)
            } else {
                None
            }
        })
        .collect();

    // Find a port not already reserved by a live process
    let port = loop {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // release so the actual service can bind
        if !live_ports.contains(&port) {
            break port;
        }
    };

    // Rewrite file: keep live entries only, append new reservation
    let mut new_content: String = content
        .lines()
        .filter(|line| {
            let mut parts = line.split_whitespace();
            let pid: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
        })
        .map(|l| format!("{l}\n"))
        .collect();
    new_content.push_str(&format!("{} {}\n", std::process::id(), port));

    file.seek(SeekFrom::Start(0)).unwrap();
    file.set_len(0).unwrap();
    file.write_all(new_content.as_bytes()).unwrap();

    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };

    port
}
