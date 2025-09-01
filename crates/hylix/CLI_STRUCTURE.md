# Hylix CLI Structure

This document describes the structure and organization of the Hylix CLI application.

## Architecture

The CLI follows a modular architecture with clear separation of concerns:

```
src/
├── main.rs              # Entry point and CLI argument parsing
├── lib.rs               # Library interface and re-exports
├── error.rs             # Error handling and custom error types
├── logging.rs           # Logging configuration and utilities
├── config.rs            # Configuration management
└── commands/            # Command implementations
    ├── mod.rs           # Command module exports
    ├── new.rs           # `hy new` command
    ├── build.rs         # `hy build` command
    ├── test.rs          # `hy test` command
    ├── run.rs           # `hy run` command
    ├── devnet.rs        # `hy devnet` command
    └── clean.rs         # `hy clean` command
```

## Key Features

### 1. Modern CLI Framework
- Uses `clap` with derive macros for argument parsing
- Supports subcommands, global flags, and help generation
- Includes shell completion support

### 2. Comprehensive Error Handling
- Custom error types with `thiserror`
- Proper error propagation and context
- User-friendly error messages

### 3. Pretty Logging
- Uses `tracing` for structured logging
- Colored output with `tracing-subscriber`
- Configurable verbosity levels
- Progress bars for long-running operations

### 4. Configuration Management
- TOML-based configuration files
- Default configuration with sensible defaults
- Per-user configuration storage

### 5. Async Support
- Full async/await support with `tokio`
- Non-blocking operations where possible
- Proper process management

## Command Structure

Each command follows a consistent pattern:

1. **Validation**: Check prerequisites and arguments
2. **Execution**: Perform the main operation
3. **Progress**: Show progress bars for long operations
4. **Logging**: Provide clear feedback to users
5. **Error Handling**: Graceful error handling and reporting

## Error Types

The CLI defines specific error types for different scenarios:

- `HylixError::Io` - File system operations
- `HylixError::Http` - Network operations
- `HylixError::Process` - External process execution
- `HylixError::Config` - Configuration issues
- `HylixError::Project` - Project-specific errors
- `HylixError::Build` - Build process errors
- `HylixError::Test` - Test execution errors
- `HylixError::Devnet` - Devnet management errors
- `HylixError::Backend` - Backend service errors
- `HylixError::Validation` - Input validation errors

## Logging Levels

- **Error**: Critical errors that prevent operation
- **Warn**: Warnings about potential issues
- **Info**: General information about operations
- **Debug**: Detailed debugging information (verbose mode)

## Configuration

Configuration is stored in `~/.config/hylix/config.toml` and includes:

- Default backend type (SP1 or Risc0)
- Scaffold repository URL
- Devnet configuration (ports, auto-start)
- Build configuration (release mode, parallel jobs)

## Extensibility

The modular structure makes it easy to:

1. Add new commands by creating new modules in `commands/`
2. Extend error types for new functionality
3. Add new configuration options
4. Implement new logging features
5. Add new validation rules

## Testing

The CLI includes:

- Unit tests for core functionality
- Integration tests for command execution
- Error handling tests
- Configuration tests

## Future Enhancements

Planned improvements include:

- Plugin system for custom commands
- Auto-update functionality
- Enhanced configuration validation
- More detailed progress reporting
- Better error recovery mechanisms
