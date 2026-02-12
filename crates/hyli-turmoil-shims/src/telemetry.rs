use opentelemetry::metrics::{Meter, MeterProvider};

mod imp {
    use super::{Meter, MeterProvider};
    use std::sync::Arc;

    #[cfg(feature = "turmoil")]
    mod turmoil {
        use super::*;
        use std::cell::RefCell;

        thread_local! {
            static THREAD_METER_PROVIDER: RefCell<Option<Arc<dyn MeterProvider + Send + Sync>>> =
                const { RefCell::new(None) };
        }

        pub fn init_global_meter_provider<P>(provider: P) -> Arc<dyn MeterProvider + Send + Sync>
        where
            P: MeterProvider + Send + Sync + Clone + 'static,
        {
            let provider = Arc::new(provider);
            THREAD_METER_PROVIDER.with(|cell| {
                *cell.borrow_mut() = Some(provider.clone());
            });
            provider
        }

        pub fn global_meter_provider_or_panic() -> Arc<dyn MeterProvider + Send + Sync> {
            THREAD_METER_PROVIDER.with(|cell| {
                cell.borrow().clone().unwrap_or_else(|| {
                    panic!("OTLP meter accessed but global meter provider is not initialized")
                })
            })
        }
    }

    #[cfg(not(feature = "turmoil"))]
    mod non_turmoil {
        use super::*;
        use std::sync::OnceLock;

        static GLOBAL_METER_PROVIDER: OnceLock<Option<Arc<dyn MeterProvider + Send + Sync>>> =
            OnceLock::new();

        pub fn init_global_meter_provider<P>(provider: P) -> Arc<dyn MeterProvider + Send + Sync>
        where
            P: MeterProvider + Send + Sync + Clone + 'static,
        {
            opentelemetry::global::set_meter_provider(provider.clone());
            let provider = Arc::new(provider);
            let _ = GLOBAL_METER_PROVIDER.set(Some(provider.clone()));
            eprintln!("[hyli-turmoil-shims] global meter provider initialized");
            provider
        }

        pub fn global_meter_provider_or_panic() -> Arc<dyn MeterProvider + Send + Sync> {
            if let Some(Some(provider)) = GLOBAL_METER_PROVIDER.get() {
                return provider.clone();
            }

            #[cfg(feature = "otlp")]
            if is_test_process() {
                let provider = GLOBAL_METER_PROVIDER.get_or_init(|| {
                    eprintln!("[hyli-turmoil-shims] initializing test meter provider");
                    Some(Arc::new(
                        opentelemetry_sdk::metrics::SdkMeterProvider::default(),
                    ))
                });
                return provider
                    .as_ref()
                    .expect("global meter provider is initialized for tests")
                    .clone();
            }

            panic!("OTLP meter accessed but global meter provider is not initialized");
        }

        #[cfg(feature = "otlp")]
        #[cfg(test)]
        fn is_test_process() -> bool {
            true
        }

        #[cfg(feature = "otlp")]
        #[cfg(not(test))]
        fn is_test_process() -> bool {
            let auto_init_env = std::env::var("CARGO_AUTO_INIT_OTLP_GLOBAL_METER").ok();
            let auto_init_enabled = match auto_init_env.as_deref() {
                Some("0") | Some("false") | Some("FALSE") | Some("False") => false,
                Some(_) => true,
                None => false,
            };
            let is_test_env = cfg!(test)
                || std::env::var("RUST_TEST_THREADS").is_ok()
                || std::env::var("NEXTEST_EXECUTION").is_ok()
                || std::env::args().any(|arg| arg.starts_with("--test-threads"));
            let is_test = is_test_env || auto_init_enabled;

            if auto_init_enabled {
                eprintln!(
                    "[hyli-turmoil-shims] warning: OTLP meter auto-init enabled via \
CARGO_AUTO_INIT_OTLP_GLOBAL_METER; initialize the global meter provider explicitly \
if this is unexpected"
                );
            } else if is_test_env {
                eprintln!(
                    "[hyli-turmoil-shims] warning: OTLP meter auto-init enabled due to \
test environment detection; initialize the global meter provider explicitly in tests \
if this is unexpected"
                );
            }

            is_test
        }
    }

    #[cfg(not(feature = "turmoil"))]
    pub use non_turmoil::{global_meter_provider_or_panic, init_global_meter_provider};
    #[cfg(feature = "turmoil")]
    pub use turmoil::{global_meter_provider_or_panic, init_global_meter_provider};

    pub fn global_meter_or_panic() -> Meter {
        global_meter_provider_or_panic().meter("hyli")
    }
}

pub use imp::{global_meter_or_panic, global_meter_provider_or_panic, init_global_meter_provider};

#[cfg(feature = "otlp")]
pub fn init_test_meter_provider() -> std::sync::Arc<dyn MeterProvider + Send + Sync> {
    init_global_meter_provider(opentelemetry_sdk::metrics::SdkMeterProvider::default())
}
