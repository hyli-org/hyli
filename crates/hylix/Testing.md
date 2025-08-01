# 🧪 Testing with Hylix

> Write, run, and automate tests for your vApp with confidence.

---

## 📦 Types of Tests

Hylix supports different layers of testing depending on what you want to validate:

| Type           | Description                                                   | Tooling used        |
| -------------- | ------------------------------------------------------------- | ------------------- |
| **Unit tests** | Test logic in isolation (e.g. zk program functions)           | `cargo test`        |
| **E2E tests**  | Test your full vApp with a local chain, provers, and backend | `hy test` + devnet |

---

## ✅ Writing Contract Unit Tests

Unit tests live in your zk program crate (under `contracts/mycontract/src/`) and are written in standard Rust.

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

You can also add tests to your backend server under `server/src`.

---

## 🧪 Writing E2E Tests

End-to-end tests are placed in the `tests/` folder at the root of your project.
They allow you to test:

* contract registration
* proof generation & verification
* interaction with a local chain
* API/backend behavior

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

You can use 
* [`Hyli client sdk`](https://crates.io/crates/hyle-client-sdk) to interact with the node and indexer 
*  [Hyli testing sdk]() to help you write your tests and compose with existing contracts (wallet, tokens)

---

## 🚀 Running Tests

To run **all tests** in an orchestrated environment:

```bash
hy test
```

What this does:

1. Compiles the project
2. Launches a devnet with `hy devnet`
3. Run your contract backend via `hy run`
4. Runs your test suite
5. Tears down the setup (unless `--keep-alive` is passed)

---

## 🔁 Persistent Devnet (Optional)

Keep your devnet alive between test runs for faster iterations:

```bash
hy test --keep-alive
```

---

## 🧪 Test Fixtures & Helpers

You can define shared setup logic in a `mod helpers` or `test_utils.rs` to handle your custom logic. The Hyli testing sdk
gives some helpers like:

* register accounts
* send tokens

---

## 🔮 Coming Soon

* [ ] `#[hyli_e2e_test]` procedural macro for isolated E2E tests
* [ ] Simulation mode for testnet

