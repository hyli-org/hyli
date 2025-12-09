# 🧪 Testing with Hylix

> Write, run, and automate tests for your vApp with confidence.

## 📦 Types of Tests

Hylix supports two main layers of testing:

| Type           | What it validates                                     | How to run          |
| -------------- | ----------------------------------------------------- | ------------------- |
| **Unit tests** | Contract or backend logic in isolation                | `cargo test`        |
| **E2E tests**  | Full vApp workflow with local chain, provers, backend | `hy test` + local devnet |

## ✅ Writing Contract Unit Tests

Unit tests live in your vApp's crate (e.g. `contracts/mycontract/src/`) and use normal Rust testing.

Example:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addition() {
        assert_eq!(add(2, 2), 4);
    }
}
```

Run only unit tests:

```bash
cargo test
```

You can also add tests to your backend (`server/src`).

## 🧪 Writing E2E Tests

End-to-end tests live in the `tests/` folder at the root of your project.

They allow you to validate:

- Contract registration
- Proof generation & verification
- Interaction with a local chain
- Backend & API behavior

Example file: `tests/e2e.rs`

```rust
#[test]
fn test_end_to_end_proof_verification() {
    // 1. Setup client
    // 2. Deploy the contract
    // 3. Send proof or call method
    // 4. Assert chain state
}
```

Use:

- [`Hyli client SDK`](https://crates.io/crates/hyli-client-sdk) to interact with the node / indexer
- Coming soon: `Hyli testing SDK` for helpers like registering accounts or sending tokens <!--TODO: add Hyli testing SDK link-->

## 🚀 Running Tests

Run all tests in an orchestrated environment:

```bash
hy test
```

This will:

1. Start a local devnet (`hy devnet`) if not already running
1. Compile your project (`hy build`)
1. Launch your backend (`hy run`)
1. Run tests in tests / via `cargo test`
1. Shut down the devnet and backend after completion

## 🔁 [Optional] Persistent devnet

Keep the devnet alive between test runs for faster iteration:

```bash
hy test --keep-alive
```

## 🧪 Test Fixtures & Helpers

You can define shared setup logic in a `helpers` module or `test_utils.rs`. The Hyli testing SDK gives utilities like:

- account registration
- token transfers
- composing tests with built-in contracts (wallet, tokens)
