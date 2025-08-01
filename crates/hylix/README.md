# 🧪 Hylix — Build, Test & Deploy vApps on Hyli

> The easiest way to build vApps.
> Powered by Risc0 & SP1. Designed for developers.

---

## ✨ Why Hylix?

Hylix is a modern developer toolbox and CLI (`hyl`) to build vApps on [Hyli](https://hyli.org), a privacy-preserving, proof-first blockchain leveraging SP1, Risc0 or Noir. Whether you're prototyping or going to production, Hylab gives you the smoothest path from idea to zk-rollout.

**Main benefits:**

* ✅ Zero-config project scaffolding
* 🚀 Built-in SP1 and Risc0 support (with Noir and more soon)
* 🧪 End-to-end testing made simple
* 🧱 Easy setup local devnet with provers & explorer
* 🔐 Prover network integration (no local proving infra needed)

---

## 🚀 Getting Started

Install the CLI:

```bash
cargo install hyl
```

Then run:

```bash
hy new my-vapp
cd my-vapp
hy build
hy devnet
hy test
```

That’s it — you’re building on Hyli.

---

## 🧰 CLI Reference

### `hy new [PROJECT]`

Scaffold a new Hyli vApp project.

* Ask to choose SP1 or Risc0 as backend
* Clones the default vApp template
* (soon) Try to validate & setup your local dev environment (Rust, risc0, sp1 toolchains...)
* Noir & Cairo coming soon

```bash
hy new my-vapp
```

#### 🧱 Project Structure

A Hylix vApp project is made of three main components:

* 📜 **contracts/**: ZK program written in Rust (using SP1 or Risc0 SDK)
* 🧠 **server/**: Your vApp’s backend, runs locally with `hy run`
  * By default includes:
    * 📝 Register contract at startup
    * ✅ Proof auto-generation
    * 📇 Contract-specific indexing
    * 🧩 Optional custom logic & APIs
* 🎨 **front/**: Frontend interface powered by **Bun** and **Vite** (optional)

Each part is optional — you can build CLI-only vApps, headless backends, or full dApps.

---

### `hy build`

Build the project.

```bash
hy build
```

---

### `hy clean`

Clean the project build artifacts.

```bash
hy clean
```

---

### `hy test`

Run your vApp’s **end-to-end tests** in a fully orchestrated local Hyli environment.

```bash
hy test
```

Execute unit & e2e tests, see [Testing](Testing.md) page for more.

#### Key Features

* ✅ Runs contract unit tests
* 🧪 Supports full E2E workflows (from proving to verification)
* ⚙️ Fully integrated with `cargo test` or custom test runners

#### What happens under the hood for e2e tests:

1. Starts `hy devnet` if not already running
2. Compiles your project (`hy build`)
3. Runs your application backend `hy run`
4. Runs tests defined in `tests/` using `cargo test`
5. Shuts down the devnet & backend after completion (unless `--keep-alive` is set)

#### Example:

```bash
hy test
```

Want to keep the devnet alive after tests?

```bash
hy test --keep-alive
```

---

### `hy devnet`

Launch a local devnet with:

* Node
* Oranj token contract & Auto-Provers
* Wallet app & Auto-Provers
* Indexer
* Explorer
* Pre-funded test accounts

```bash
hy devnet
```

Want to pause the network ?

```bash 
hy devnet stop 
hy devnet start
```

Want a fresh state?

```bash
hy devnet --reset
```

#### Soon

Want to fork a running network ?

```bash 
hy devnet fork [ENDPOINT]
```

---

### `hy run`

Start your backend service locally or on testnet.
The app backend **registers your contract**, **runs a local auto-prover**, and launches core modules like the **contract indexer**. You can customize the backend in the `server/` folder.

```bash
hy run
```

By default, `hy run` operates in local dev mode.

#### Options

* `--testnet`: Register and interact with contracts on the public Hyli testnet.
* `--watch`: Automatically rebuild and re-register on file changes (coming soon)

#### What it does (under the hood):

* ✅ Registers your vApp contract on-chain
* 🔁 Starts a local auto-prover (generates and posts proofs when needed)
* 📇 Launches a contract indexer to track state transitions
* 🛠️ Wires everything together for a ready-to-use dev backend

#### Testnet mode (soon):

```bash
hy run --testnet
```

This will:

* Start the backend connected to the testnet
* Ask to upload your contract on the prover network


---

## 📡 Upload to the Prover Network (Soon)

Upload your compiled ELF to the **Hyli Prover Network**, allowing proofs to be generated off-chain by a prover network.

This is especially useful on testnet where you want to avoid setting up local proving infrastructure.

```bash
hy upload
```

**What it does:**

* Validates the ELF format
* Register the contract on the prover network given the ELFs

**Why use it?**

* ⚡ Avoid local proving (faster dev loop)
* 🌍 Share proof artifacts with teammates or collaborators
* 🧱 Make your app testnet-ready without running provers


---

## 🧠 Under the Hood

Hylix builds on top of:

* **SP1/Risc0 zkVM** for fast, verifiable compute
* CairoM for client side verifiable compute
* Noir for client side privacy (soon)
* **Rust** for native speed and tooling compatibility
* **Bun**, **vite**, vue3 & tailwind for frontend application

Coming soon:

* 🧑‍🎨 Noir Integration (Q4 2025)
* 🌀 Cairo Exploration
* 📦 Custom Prover Uploads via `hy upload`

---

## 🧪 Try It Out

We’re just getting started. If you're testing Hylix early:

* Open issues or ideas [here](https://github.com/hyli-org/hyli/issues)
* Share feedback with the Hyli team
* Ping us with questions!

---

## 🛤️ Whishlist

* [ ] Noir support
- [ ] Tool auto-upgrade
* [ ] Cairo experiments
* [ ] Plugin system for custom commands
- [ ] Test proc-macro for isolated e2e testing

---

## ❤️ Built with Love by the Hyli Team

---


