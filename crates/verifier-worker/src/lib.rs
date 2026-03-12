use anyhow::{Context, Result};
use borsh::to_vec;
use hyli_model::verifier_worker::{VerifyRequest, VerifyResponse};
use std::future::Future;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub fn init_worker_tracing(default_filter: &str) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .with_writer(std::io::stderr)
        .try_init();
    Ok(())
}

pub async fn run_worker_loop<R, W, H, Fut>(
    mut reader: R,
    mut writer: W,
    mut handler: H,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    H: FnMut(VerifyRequest) -> Fut,
    Fut: Future<Output = Result<VerifyResponse>>,
{
    loop {
        let request_len = match reader.read_u32_le().await {
            Ok(len) => len,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err).context("reading request length"),
        };

        let mut request_bytes = vec![0; request_len as usize];
        reader
            .read_exact(&mut request_bytes)
            .await
            .context("reading request body")?;
        let request =
            borsh::from_slice::<VerifyRequest>(&request_bytes).context("deserializing request")?;

        let response = handler(request).await.unwrap_or_else(|err| VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("{err:#}"),
        });
        let response_bytes = to_vec(&response).context("serializing response")?;

        writer
            .write_u32_le(response_bytes.len() as u32)
            .await
            .context("writing response length")?;
        writer
            .write_all(&response_bytes)
            .await
            .context("writing response body")?;
        writer.flush().await.context("flushing response")?;
    }

    Ok(())
}
