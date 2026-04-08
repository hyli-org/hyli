use hyli_crypto::BlstCrypto;
use hyli_model::SignedByValidator;
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

struct CountingAlloc;

static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOC: CountingAlloc = CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() && new_size > layout.size() {
            ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

type Data = String;

const SIZES: &[usize] = &[1, 4, 16, 64];
const ITERS: usize = 500;

fn main() {
    println!("hyli-crypto aggregate allocation bench");
    println!(
        "{:<16} {:>6} {:>14} {:>14}",
        "case", "n", "bytes/op", "ns/op"
    );

    for &n in SIZES {
        let (crypto, signed) = build_inputs(n);
        report(
            "aggregate",
            n,
            measure(ITERS, || {
                black_box(
                    BlstCrypto::aggregate(Data::default(), signed.iter())
                        .expect("aggregate should succeed"),
                );
            }),
        );

        report(
            "sign_aggregate",
            n,
            measure(ITERS, || {
                black_box(
                    crypto
                        .sign_aggregate(Data::default(), signed.iter())
                        .expect("sign_aggregate should succeed"),
                );
            }),
        );
    }
}

fn build_inputs(n: usize) -> (BlstCrypto, Vec<SignedByValidator<Data>>) {
    let message = Data::default();
    let signers = (0..=n)
        .map(|i| {
            BlstCrypto::new_deterministic(&format!("bench-validator-{n}-{i}"))
                .expect("deterministic crypto")
        })
        .collect::<Vec<_>>();

    let signed = signers[..n]
        .iter()
        .map(|crypto| crypto.sign(message.clone()).expect("sign should succeed"))
        .collect::<Vec<_>>();

    (signers[n].clone(), signed)
}

fn measure(mut iterations: usize, mut f: impl FnMut()) -> Sample {
    f();

    let start_bytes = allocated_bytes();
    let start = Instant::now();
    while iterations > 0 {
        f();
        iterations -= 1;
    }

    Sample {
        bytes_per_op: (allocated_bytes() - start_bytes) as f64 / ITERS as f64,
        ns_per_op: start.elapsed().as_nanos() as f64 / ITERS as f64,
    }
}

fn allocated_bytes() -> u64 {
    ALLOCATED_BYTES.load(Ordering::Relaxed)
}

fn report(label: &str, n: usize, sample: Sample) {
    println!(
        "{:<16} {:>6} {:>14.2} {:>14.2}",
        label, n, sample.bytes_per_op, sample.ns_per_op
    );
}

struct Sample {
    bytes_per_op: f64,
    ns_per_op: f64,
}
