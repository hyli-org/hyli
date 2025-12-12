pub fn extract(headers: Vec<(String, String)>) -> opentelemetry::Context {
    #[cfg(feature = "instrumentation")]
    {
        use std::collections::HashMap;

        use opentelemetry::global;
        let map = headers.into_iter().collect::<HashMap<String, String>>();

        tracing::info!("💚 Extracting tracing context from {} headers", map.len());

        global::get_text_map_propagator(move |propagator| propagator.extract(&map))
    }
    #[cfg(not(feature = "instrumentation"))]
    {
        opentelemetry::Context::new()
    }
}
