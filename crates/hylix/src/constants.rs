/// Docker container names for the devnet environment
pub mod containers {
    pub const NODE: &str = "hyli-devnet-node";
    pub const POSTGRES: &str = "hyli-devnet-postgres";
    pub const INDEXER: &str = "hyli-devnet-indexer";
    pub const WALLET: &str = "hyli-devnet-wallet";
    pub const WALLET_UI: &str = "hyli-devnet-wallet-ui";
    pub const REGISTRY: &str = "hyli-devnet-registry";
    pub const REGISTRY_UI: &str = "hyli-devnet-registry-ui";
    pub const ALL: [&str; 7] = [
        NODE,
        POSTGRES,
        INDEXER,
        WALLET,
        WALLET_UI,
        REGISTRY,
        REGISTRY_UI,
    ];
}

/// Docker network names
pub mod networks {
    pub const DEVNET: &str = "hyli-devnet";
}

/// Docker image names
pub mod images {
    pub const POSTGRES: &str = "postgres:17";
}

/// Default passwords used in the system
pub mod passwords {
    pub const DEFAULT: &str = "hylisecure";
}

/// Feature flags
pub mod features {
    pub const NONREPRODUCIBLE: &str = "nonreproducible";
}

/// Environment variable names
pub mod env_vars {
    pub const RISC0_DEV_MODE: &str = "RISC0_DEV_MODE";
    pub const SP1_PROVER: &str = "SP1_PROVER";
    pub const HYLI_NODE_URL: &str = "HYLI_NODE_URL";
    pub const HYLI_INDEXER_URL: &str = "HYLI_INDEXER_URL";
    pub const HYLI_DA_READ_FROM: &str = "HYLI_DA_READ_FROM";
    pub const HYLI_REGISTRY_URL: &str = "HYLI_REGISTRY_URL";
    pub const HYLI_REGISTRY_API_KEY: &str = "HYLI_REGISTRY_API_KEY";
    pub const HYLI_DATABASE_URL: &str = "HYLI_DATABASE_URL";
    pub const HYLI_RUN_INDEXER: &str = "HYLI_RUN_INDEXER";
    pub const HYLI_RUN_EXPLORER: &str = "HYLI_RUN_EXPLORER";
    pub const RUST_LOG: &str = "RUST_LOG";
}

/// Environment variable values
pub mod env_values {
    pub const RISC0_DEV_MODE_TRUE: &str = "true";
    pub const RISC0_DEV_MODE_ONE: &str = "1";
    pub const SP1_PROVER_MOCK: &str = "mock";
    pub const REGISTRY_API_KEY_DEV: &str = "dev";
}
