#[cfg_attr(not(feature = "instrumentation"), expect(clippy::unused_variables))]
pub fn extract(headers: Vec<(String, String)>) -> opentelemetry::Context {
    #[cfg(feature = "instrumentation")]
    {
        use std::collections::HashMap;

        use opentelemetry::global;
        let map = headers.into_iter().collect::<HashMap<String, String>>();

        global::get_text_map_propagator(move |propagator| propagator.extract(&map))
    }
    #[cfg(not(feature = "instrumentation"))]
    {
        opentelemetry::Context::new()
    }
}
