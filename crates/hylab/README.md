
# 🧪 Hylab — Build, Test & Deploy ZK Apps on Hyli

> The easiest way to build on Hyli.
> Powered by Risc0 & SP1. Designed for zk developers.

---

## ✨ Why Hylab?

Hylab is a modern developer toolbox and CLI (`hyl`) to build zkApps on [Hyli](https://hyli.org), a privacy-preserving, proof-first blockchain leveraging SP1, Risc0 or Noir. Whether you're prototyping or going to production, Hylab gives you the smoothest path from idea to zk-rollout.

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
hyl init my-zkapp
cd my-zkapp
hyl build
hyl net
hyl test
```

That’s it — you’re building on Hyli.

---

## 🧰 CLI Reference

### `hyl init [PROJECT]`

Scaffold a new Hyli project.

* Clones the default zkApp template
* Choose SP1 or Risc0 as backend
* Noir & Cairo coming soon

```bash
hyl init my-zkapp
```

---

### `hyl build`

Build the project using Cargo under the hood.

```bash
hyl build
```

---

### `hyl clean`

Clean the project build artifacts.

```bash
hyl clean
```

---

### `hyl net`

Launch a local devnet with:

* Node
* Oranj token contract & Auto-Provers
* Wallet app & Auto-Provers
* Indexer
* Explorer
* Pre-funded test accounts

```bash
hyl net
```

Want a fresh state?

```bash
hyl net --reset
```

---

### `hyl test`

Run your end-to-end zkApp tests in a local Hyli environment.

```bash
hyl test
```

It starts the devnet with `hyl net` behind the scenes, then runs your tests.

---

## 🧠 Under the Hood

Hylab builds on top of:

* **SP1/Risc0 zkVM** for fast, verifiable compute
* **Rust** for native speed and tooling compatibility

Coming soon:

* 🧑‍🎨 Noir Integration (Q4 2025)
* 🌀 Cairo Exploration
* 📦 Custom Prover Uploads via `hyl upload`

---

## 📡 Upload to the Prover Network

Upload your compiled ELF to the **Hyli Prover Network**, allowing proofs to be generated off-chain by a prover network.

This is especially useful on testnet where you want to avoid setting up local proving infrastructure.

```bash
hyl upload
```

**What it does:**

* Validates the ELF format
* Register the contract on the prover network given the ELFs

**Why use it?**

* ⚡ Avoid local proving (faster dev loop)
* 🌍 Share proof artifacts with teammates or collaborators
* 🧱 Make your app testnet-ready without running provers


---

## 🧪 Try It Out

We’re just getting started. If you're testing Hylab early:

* Open issues or ideas [here](https://github.com/hyli-org/hyli/issues)
* Share feedback with the Hyli team
* Ping us with questions!

---

## 🛤️ Roadmap

* [ ] Noir support
* [ ] Cairo experiments
* [ ] Plugin system for custom commands
* [ ] Cross-project library support
* [ ] zkApp deployment templates

---

## ❤️ Built with Love by the Hyli Team

---


