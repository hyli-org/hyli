#![allow(clippy::unwrap_used, clippy::expect_used)]
use fixtures::ctx::E2ECtx;

mod fixtures;

use anyhow::Result;

mod e2e_metrics {

    use tracing::info;

    use super::*;

    async fn poll_metrics(ctx: &E2ECtx) -> Result<()> {
        let metrics = ctx.metrics().await?;
        info!("-- polled metrics {}", metrics);

        if !metrics.contains("receive_") && !metrics.contains("send_") {
            return Err(anyhow::anyhow!("Metrics not found"));
        }
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_metrics_are_present_on_endpoint() -> Result<()> {
        let ctx = E2ECtx::new_single(500).await?;

        let start = std::time::Instant::now();
        loop {
            match poll_metrics(&ctx).await {
                Ok(_) => break Ok(()),
                Err(_) if start.elapsed().as_secs() < 5 => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => break Err(e),
            }
        }
    }
}
