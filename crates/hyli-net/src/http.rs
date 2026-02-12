use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
#[cfg(feature = "instrumentation")]
use axum::http;
use bytes::Buf;
use http_body_util::BodyExt;
use hyper::{body::Incoming, client::conn::http1, Method, Request, Response, Uri};
use hyper_util::rt::TokioIo;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{error, warn};

pub use hyper::http::StatusCode;

pub enum ContentType {
    Text,
    Json,
}

#[derive(Clone)]
pub struct HttpClient {
    pub url: Uri,
    pub api_key: Option<String>,
    pub retry: Option<(usize, Duration)>,
}
impl HttpClient {
    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self, body, content_type))
    )]
    pub async fn request<T>(
        &self,
        endpoint: &str,
        method: Method,
        content_type: ContentType,
        body: Option<&T>,
    ) -> anyhow::Result<Response<Incoming>>
    where
        T: Serialize,
    {
        let full_url = format!("{}{}", &self.url, endpoint);
        let uri: Uri = full_url.parse().context("Parsing URI")?;

        let authority = uri.authority().context("URI Authority")?.clone();
        let host = authority.host().to_string();
        let port = authority.port_u16().unwrap_or(80);
        let addr = format!("{host}:{port}");
        let stream = crate::net::TcpStream::connect(addr)
            .await
            .context("TCP connection")?;
        let io = TokioIo::new(stream);

        let (mut sender, connection) = http1::handshake(io).await.context("Handshake HTTP/1")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error {:?}", e);
            }
        });

        let content_type_header = match content_type {
            ContentType::Text => "application/text",
            ContentType::Json => "application/json",
        };

        let mut req_builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(hyper::header::CONTENT_TYPE, content_type_header);

        if let Some(ref key) = self.api_key {
            req_builder = req_builder.header("X-API-KEY", key);
        }

        #[cfg(feature = "instrumentation")]
        if let Some(headers) = req_builder.headers_mut() {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                use tracing_opentelemetry::OpenTelemetrySpanExt;
                let context = tracing::Span::current().context();
                propagator.inject_context(&context, &mut HeaderMapInjector(headers))
            });
        }

        let request = if let Some(b) = body {
            let json_body = serde_json::to_string(b).context("Serializing request body")?;

            req_builder.body(json_body).context("Building request")?
        } else {
            req_builder.body("".to_string())?
        };

        let response = sender
            .send_request(request)
            .await
            .context("Sending request")?;

        if !response.status().is_success() {
            let body = Self::parse_response_text(response).await?;
            anyhow::bail!(body);
        }

        Ok(response)
    }

    async fn parse_response_text(response: Response<Incoming>) -> anyhow::Result<String> {
        let body = response.into_body();

        let response = body.collect().await.context("Collecting body bytes")?;

        let str_bytes: bytes::Bytes = response.to_bytes();
        let str = String::from_utf8(str_bytes.to_vec())?;

        Ok(str)
    }

    async fn parse_response_json<T: serde::de::DeserializeOwned>(
        response: Response<Incoming>,
    ) -> anyhow::Result<T> {
        let body = response.into_body();

        let response = body
            .collect()
            .await
            .context("Collecting body bytes")?
            .aggregate();

        let result =
            serde_json::from_reader(response.reader()).context("Deserializing response body")?;
        Ok(result)
    }

    async fn retry<F, Fut, R>(&self, do_request: F) -> Result<R>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = anyhow::Result<R>>,
    {
        match self.retry {
            Some((n, duration)) => {
                let mut inner_n = n;

                loop {
                    match do_request().await {
                        Ok(res) => break Ok(res),
                        Err(e) if inner_n > 0 => {
                            warn!(
                                "Error when doing request, waiting {} millis before retrying: {}",
                                duration.as_millis(),
                                e
                            );
                            inner_n -= 1;
                            tokio::time::sleep(duration).await;
                        }
                        Err(e) => {
                            // Stop retrying
                            break Err(e).context("Client errored after {} retries, stopping now.");
                        }
                    }
                }
            }
            None => do_request().await,
        }
    }

    pub async fn get<R>(&self, endpoint: &str) -> anyhow::Result<R>
    where
        R: DeserializeOwned,
    {
        let do_request = async || {
            self.request::<String>(endpoint, Method::GET, ContentType::Json, None)
                .await
        };
        let response = self.retry(do_request).await?;
        Self::parse_response_json(response).await
    }

    pub async fn get_str(&self, endpoint: &str) -> anyhow::Result<String> {
        let do_request = async || {
            self.request::<String>(endpoint, Method::GET, ContentType::Text, None)
                .await
        };
        let response = self.retry(do_request).await?;
        Self::parse_response_text(response).await
    }

    pub async fn post_json<T, R>(&self, endpoint: &str, body: &T) -> anyhow::Result<R>
    where
        R: DeserializeOwned,
        T: Serialize,
    {
        let do_request = async || {
            self.request::<T>(endpoint, Method::POST, ContentType::Json, Some(body))
                .await
        };
        let response = self.retry(do_request).await?;
        Self::parse_response_json(response).await
    }
}

#[cfg(feature = "instrumentation")]
struct HeaderMapInjector<'a>(&'a mut http::HeaderMap);

#[cfg(feature = "instrumentation")]
impl opentelemetry::propagation::Injector for HeaderMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        use axum::http::{HeaderName, HeaderValue};

        match HeaderName::from_bytes(key.as_bytes()) {
            Ok(key) => match HeaderValue::try_from(&value) {
                Ok(value) => {
                    self.0.insert(key, value);
                }

                Err(error) => warn!(value, error =? error, "parse metadata value"),
            },

            Err(error) => warn!(key, error =? error, "parse metadata key"),
        }
    }
}
