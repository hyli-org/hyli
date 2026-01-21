#![cfg(feature = "turmoil")]
#![cfg(test)]

use std::sync::mpsc;
use std::time::Duration;

use hyli_net::tcp::intercept::{set_message_hook_scoped, MessageAction};
use hyli_net::tcp::{decode_tcp_payload, tcp_client::TcpClient, tcp_server::TcpServer, TcpEvent};
use sdk::{DataProposal, Transaction};

#[test_log::test]
fn turmoil_drop_mempool_data_proposals() -> anyhow::Result<()> {
    let mut sim = hyli_net::turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(5))
        .tick_duration(Duration::from_millis(50))
        .enable_tokio_io()
        .build();

    let (result_tx, result_rx) = mpsc::channel();

    let mut drops_left = 2usize;
    let _intercept = set_message_hook_scoped(move |bytes| {
        if decode_tcp_payload::<DataProposal>(bytes).is_ok() && drops_left > 0 {
            drops_left -= 1;
            return MessageAction::Drop;
        }
        MessageAction::Pass
    });

    let proposal1 = DataProposal::new(b"parent-1".into(), vec![Transaction::default()]);
    let proposal2 = DataProposal::new(b"parent-2".into(), vec![Transaction::default()]);
    let proposal3 = DataProposal::new(b"parent-3".into(), vec![Transaction::default()]);
    let proposal3_for_client = proposal3.clone();

    sim.client("server", async move {
        let mut server =
            TcpServer::<DataProposal, DataProposal>::start(4000, "MempoolServer").await?;
        loop {
            if let Some(TcpEvent::Message { data, .. }) = server.listen_next().await {
                let _ = result_tx.send(data);
                break;
            }
        }
        Ok(())
    });

    sim.client("client", async move {
        let mut client =
            TcpClient::<DataProposal, DataProposal>::connect("client", "server:4000").await?;
        client.send(proposal1).await?;
        client.send(proposal2).await?;
        client.send(proposal3_for_client).await?;
        Ok(())
    });

    sim.run()
        .map_err(|e| anyhow::anyhow!("Simulation error {e}"))?;

    let received = result_rx.recv_timeout(Duration::from_secs(1))?;
    assert_eq!(received, proposal3);

    Ok(())
}
