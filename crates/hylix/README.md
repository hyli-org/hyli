# Hylix

<div align="center">

  _Build, test & deploy verifiable apps on Hyli._

  **Hyli** is the easiest way to build vApps. Powered by Risc0 & SP1. Designed for developers.
</div>

## :test_tube: Why Hylix?

Hylix is a developer toolbox and CLI (`hy`) to build vApps on [Hyli](https://hyli.org), the new proof-powered L1 to build the next generation of apps onchain.

### Who is this for?

Developers who are building vApps on the Hyli blockchain and have prior knowledge of Rust and CLI usage.

### Main benefits

* ✅ Zero-config project scaffolding
* 🚀 Built-in SP1 and Risc0 support (with Noir and more soon)
* 🧪 End-to-end testing made simple
* 🧱 Easy local node setup with explorer
* 🔐 No proving infra needed

## 🧪 Trying this out

We’re just getting started. If you're testing Hylix early:

* Share [issues or ideas](https://github.com/hyli-org/hyli/issues) in this repository
* [Ping the Hyli team](https://t.me/hyli_org) with feedback or questions

## 🚀 Getting started

Install the CLI:

```sh
cargo install hylix
```

Then run:

```sh
hy new my-vapp
cd my-vapp
hy build
hy devnet
hy test
```

## 🧰 CLI reference

### Cheatsheet

| Command   | Action           |
|-----------|------------------|
| [`hy new [PROJECT]`](#hy-new-project)    | Create a project using [the scaffold](https://github.com/hyli-org/app-scaffold). |
| [`hy build`](#hy-build)  | Build contracts.  |
| [`hy test`](#hy-test)   | Run E2E tests.    |
| [`hy run`](#hy-run)    | Start backend.    |
| [`hy devnet`](#hy-devnet) | Start local node.  |

### `hy new [PROJECT]`

Generate a new project:

```sh
hy new my-vapp
```

* You will be asked to choose SP1 or Risc0 for your backend
* Clone the default vApp scaffold

<details>
  <summary>Coming soon:</summary>

* More proving schemes
* Validate & setup your local dev environment (Rust, risc0, sp1 toolchains...)

</details>

<details>
  <summary>Scaffold structure</summary>

A Hylix vApp project is made of three main components:

* 📜 **contracts/**: ZK program written in Rust (using SP1 or Risc0 SDK)
* 🧠 **server/**: Your vApp’s backend, runs locally with `hy run`
  * By default includes:
    * 📝 Register contract at startup
    * ✅ Proof auto-generation
    * 📇 Contract-specific indexing
    * 🧩 Optional custom logic & APIs
* 🎨 **front/**: Frontend interface powered by **Bun** and **Vite** (optional)

Each part is optional: you can build CLI-only vApps, headless backends, or full dApps.
</details>

Read more: [Scaffold repo](https://github.com/hyli-org/app-scaffold/) | [Quickstart in docs](https://docs.hyli.org/quickstart/edit/).

### `hy build`

Build the project:

```sh
hy build
```

Then, clean the project build artifacts:

```sh
hy clean
```

### `hy devnet`

Launch a local devnet with:

* Node
* Oranj token contract & Auto-Provers
* Wallet app & Auto-Provers
* Indexer
* Explorer
* Pre-funded test accounts

```sh
hy devnet
```

Want to pause the network ?

```sh
hy devnet stop 
hy devnet start
```

Want a fresh state?

```sh
hy devnet --reset
```

#### Soon

Want to fork a running network ?

```sh
hy devnet fork [ENDPOINT]
```

### `hy test`

Run your vApp’s **end-to-end tests** in a fully orchestrated local Hyli environment.

```sh
hy test
```

[Read more on executing unit & E2E tests](Testing.md).

#### Key Features

* ✅ Contract unit tests
* 🧪 Full E2E workflows (from proving to verification)
* ⚙️ Full integration with `cargo test` or custom test runners

#### What `hy test` does (under the hood)

1. Starts `hy devnet` if not already running
2. Compiles your project (`hy build`)
3. Runs your application backend `hy run`
4. Runs tests defined in `tests/` using `cargo test`
5. Shuts down the devnet & backend after completion (unless `--keep-alive` is set)

#### Example

```sh
hy test
```

Want to keep the devnet alive after tests?

```sh
hy test --keep-alive
```

### `hy run`

Start your backend service locally or on testnet.
The app backend **registers your contract**, **runs a local auto-prover**, and launches core modules like the **contract indexer**. You can customize the backend in the `server/` folder.

```sh
hy run
```

By default, `hy run` operates in local dev mode.

#### Options

* `--testnet`: Register and interact with contracts on the public Hyli testnet.
* `--watch`: Automatically rebuild and re-register on file changes (coming soon)

#### What `hy run` does (under the hood)

* ✅ Registers your vApp contract on-chain
* 🔁 Starts a local auto-prover (generates and posts proofs when needed)
* 📇 Launches a contract indexer to track state transitions
* 🛠️ Wires everything together for a ready-to-use dev backend

## 🧠 Hylix components

Hylix builds on top of:

* **SP1/Risc0 zkVM** for fast, verifiable compute
* **Rust** for native speed and tooling compatibility
* **Bun**, **vite**, vue3 & tailwind for frontend application

## 🛤️ Roadmap

* [ ] Noir + Cairo support
* [ ] Tool auto-upgrade
* [ ] Plugins for custom commands
* [ ] Test proc-macro for isolated E2E testing
* [ ] Upload to a prover network (`hy upload`): upload your compiled ELF to a prover network without setting up lovcal proving infrastructure
* [ ] Testnet mode (`hy run --testnet`) to start a backend and deploy the contract on a prover network

## :link: Links

<div align="center">
    <p>
        📚 <a href="https://hyli.org/">Website</a> | <a href="https://docs.hyli.org">Docs</a> | <a href="https://docs.hyli.org/guide/">Hyli Guide</a> | <a href="https://docs.hyli.org/quickstart/">Quickstart</a> | <a href="https://docs.hyli.org/tooling/">Tooling</a>
    </p>
    <p>
        Follow <a href="https://twitter.com/hyli_org">on X</a> | <a href="https://www.linkedin.com/company/hyli-org">LinkedIn</a> | <a href="https://t.me/hyli_org">Telegram</a> | <a href="https://www.youtube.com/@hyli-org">YouTube</a> | <a href="https://blog.hyli.org/">Blog &amp; Newsletter</a>
    </p>
</div>
