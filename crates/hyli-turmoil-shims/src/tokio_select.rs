// Optional biased tokio::select! for deterministic turmoil tests.
// Define the macro conditionally in this crate so call sites do not inherit a
// `cfg(feature = "turmoil")` check in downstream crates.
#[cfg(feature = "turmoil")]
#[macro_export]
macro_rules! tokio_select_biased {
    ($($tt:tt)*) => {{
        tokio::select! {
            biased;
            $($tt)*
        }
    }};
}

#[cfg(not(feature = "turmoil"))]
#[macro_export]
macro_rules! tokio_select_biased {
    ($($tt:tt)*) => {{
        tokio::select! {
            $($tt)*
        }
    }};
}
