// Optional biased tokio::select! for deterministic turmoil tests.
#[macro_export]
macro_rules! tokio_select_biased {
    ($($tt:tt)*) => {{
        #[cfg(feature = "turmoil")]
        {
            tokio::select! {
                biased;
                $($tt)*
            }
        }
        #[cfg(not(feature = "turmoil"))]
        {
            tokio::select! {
                $($tt)*
            }
        }
    }};
}
