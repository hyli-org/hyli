//! Public API for interacting with the node.

use crate::{
    bus::SharedMessageBus, log_error, module_bus_client, module_handle_messages, modules::Module,
};
use anyhow::{Context, Result};
pub use axum::Router;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{header, Request, Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
    routing::get,
    Json,
};
use sdk::{api::NodeInfo, *};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[cfg(feature = "rest")]
pub use client_sdk::rest_client as client;
pub use client_sdk::AppError;

module_bus_client! {
    struct RestBusClient {
    }
}

pub struct RestApiRunContext {
    pub port: u16,
    pub info: NodeInfo,
    pub router: Router,
    pub max_body_size: usize,
    pub openapi: utoipa::openapi::OpenApi,
}

impl RestApiRunContext {
    pub fn new(
        port: u16,
        info: NodeInfo,
        router: Router,
        max_body_size: usize,
        openapi: utoipa::openapi::OpenApi,
    ) -> RestApiRunContext {
        Self {
            port,
            info,
            router,
            max_body_size,
            openapi,
        }
    }
}

pub struct RouterState {
    info: NodeInfo,
}

pub struct RestApi {
    port: u16,
    app: Option<Router>,
    bus: RestBusClient,
}

#[derive(OpenApi)]
#[openapi(
    info(
        description = "Hyli Node API",
        title = "Hyli Node API",
    ),
    // When opening the swagger, if on some endpoint you get the error:
    // Could not resolve reference: JSON Pointer evaluation failed while evaluating token "BlobIndex" against an ObjectElement
    // then it means you need to add it to this list.
    // More details here: https://github.com/juhaku/utoipa/issues/894
    components(schemas(BlobIndex, RegisterContractEffect))
)]
pub struct ApiDoc;

impl Module for RestApi {
    type Context = RestApiRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        #[cfg(feature = "instrumentation")]
        let app = ctx.router.layer(
            TraceLayer::new_for_http()
                .make_span_with(make_span)
                .on_response(close_span),
        );

        #[cfg(not(feature = "instrumentation"))]
        let app = ctx.router;

        let app = app.merge(
            Router::new()
                .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ctx.openapi))
                .route("/v1/info", get(get_info))
                .with_state(RouterState { info: ctx.info }),
        );
        let app = app
            .layer(CatchPanicLayer::custom(handle_panic))
            .layer(DefaultBodyLimit::max(ctx.max_body_size)) // 10 MB
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(TraceLayer::new_for_http())
            .layer(tower_http::decompression::RequestDecompressionLayer::new())
            .layer(axum::middleware::from_fn(request_logger));

        Ok(RestApi {
            port: ctx.port,
            app: Some(app),
            bus: RestBusClient::new_from_bus(bus.new_handle()).await,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.serve().await
    }
}

pub async fn request_logger(req: Request<Body>, next: Next) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start_time = Instant::now();

    // Passer la requête au prochain middleware ou au gestionnaire
    let response = next.run(req).await;

    let status = response.status();
    let elapsed_time = start_time.elapsed();

    // Debug log for metrics and health endpoints, info for others
    let path = uri.path();
    if path == "/_health" || path.starts_with("/v1/info") {
        tracing::debug!(
            "[{}] {} - {} ({} μs)",
            method,
            uri,
            status,
            elapsed_time.as_micros()
        );
    } else {
        info!(
            "[{}] {} - {} ({} μs)",
            method,
            uri,
            status,
            elapsed_time.as_micros()
        );
    }

    response
}

#[cfg(feature = "instrumentation")]
fn make_span<B>(request: &Request<B>) -> tracing::Span {
    use opentelemetry_http::HeaderExtractor;
    use tracing::field;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let headers = request.headers();
    let name = format!("{} {}", request.method(), request.uri());
    let span = tracing::info_span!(
        "http-request",
        name,
        ?headers,
        http.method =  %request.method(),
        http.uri =  %request.uri(),
        http.status = field::Empty,
        http.duration = field::Empty,
    );

    let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    span.set_parent(parent_context);

    span
}

#[cfg(feature = "instrumentation")]
fn close_span<B>(response: &Response<B>, latency: std::time::Duration, span: &tracing::Span) {
    span.record("http.status", tracing::field::display(response.status()));
    span.record(
        "http.duration",
        tracing::field::display(latency.as_micros()),
    );
}

pub async fn get_info(State(state): State<RouterState>) -> Result<impl IntoResponse, AppError> {
    Ok(Json(state.info))
}

impl RestApi {
    pub async fn serve(&mut self) -> Result<()> {
        info!(
            "📡  Starting {} module, listening on port {}",
            std::any::type_name::<Self>(),
            self.port
        );

        let listener = hyli_net::net::bind_tcp_listener(self.port)
            .await
            .context("Starting rest server")?;

        #[allow(
            clippy::expect_used,
            reason = "app is guaranteed to be set during initialization"
        )]
        let app = self.app.take().expect("app is not set");

        // On module shutdown, we want to shutdown the axum server and wait for its shutdown to complete.
        let axum_cancel_token = CancellationToken::new();
        let axum_server = tokio::spawn({
            let token = axum_cancel_token.clone();
            async move {
                log_error!(
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            token.cancelled().await;
                        })
                        .await,
                    "serving Axum"
                )?;
                Ok::<(), anyhow::Error>(())
            }
        });
        module_handle_messages! {
            on_self self,
            delay_shutdown_until {
                // When the module tries to shutdown it'll cancel the token
                // and then we actually exit the loop when axum is done.
                axum_cancel_token.cancel();
                axum_server.is_finished()
            },
        };

        Ok(())
    }
}

impl Clone for RouterState {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
        }
    }
}

pub fn handle_panic(err: Box<dyn std::any::Any + Send + 'static>) -> Response<String> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic message".to_string()
    };

    tracing::error!("Panic occurred in Axum route: {}", details);

    let body = serde_json::json!({
        "error": {
            "kind": "panic",
            "details": details,
        }
    });
    let body = serde_json::to_string(&body).unwrap();

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}
