# Hyli model

This crate defines the datamodel for the hyli ecosystem.

The types are defined in separated files for clarity, but are almost all re-exported at the root.

Example:

```rust
use hyle_model::ZkProgramInput; // Valid
use hyle_model::contract::ZkProgramInput; // NOT valid
```

The default feature `full` enables to access all the datamodel, this should be disabled in the contract's dependency (See [examples](https://github.com/hyli-org/examples)).

The feature `sqlx` is for hyli node internal need, you might not need it.
