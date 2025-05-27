# Hyli client sdk

This crates holds some tools for the app interacting with a smart contract.

The `risc0` & `sp1` features enables necessary implementations for the Transaction Builder. Activate
only the one relevant for your use-case.

The `rest` feature exports a `NodeApiHttpClient` and a `IndexerApiHttpClient` that allows you to call
the node of the indexer on their http endpoints.

The `tcp` feature exports a `NodeTcpClient` that allows you to send transactions to the node using tcp.
Used for loadtesting purposes.
