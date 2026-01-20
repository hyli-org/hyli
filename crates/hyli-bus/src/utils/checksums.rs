use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use rand::{distr::Alphanumeric, Rng};
use tracing::info;

use crate::utils::deterministic_rng::deterministic_rng;

/// Error type for persistence operations
#[derive(Debug)]
pub enum PersistenceError {
    /// File not found (not an error, use default)
    NotFound,
    /// Checksum verification failed (data corruption)
    ChecksumMismatch { expected: String, actual: String },
    /// File exists but is not listed in the manifest (wasn't in successful shutdown)
    FileNotInManifest(PathBuf),
    /// Checksum file format invalid
    ChecksumParseFailed(String),
    /// Deserialization failed
    DeserializationFailed(String),
    /// IO error during read/write
    IoError(std::io::Error),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "File not found"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "Data corruption detected: checksum mismatch (expected {}, got {})",
                    expected, actual
                )
            }
            Self::FileNotInManifest(path) => {
                write!(
                    f,
                    "File {} exists but is not in manifest (not from successful shutdown)",
                    path.display()
                )
            }
            Self::ChecksumParseFailed(msg) => {
                write!(f, "Invalid checksum/manifest contents: {}", msg)
            }
            Self::DeserializationFailed(msg) => write!(f, "Failed to deserialize data: {}", msg),
            Self::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl From<std::io::Error> for PersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

pub(crate) struct ChecksumWriter<W> {
    inner: W,
    hasher: crc32fast::Hasher,
}

impl<W: Write> ChecksumWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }

    pub(crate) fn checksum(&self) -> u32 {
        self.hasher.clone().finalize()
    }
}

impl<W: Write> Write for ChecksumWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.inner.write(buf)?;
        self.hasher.update(&buf[..bytes_written]);
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct ChecksumReader<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: Read> ChecksumReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }

    pub(crate) fn checksum(&self) -> u32 {
        self.hasher.clone().finalize()
    }

    pub(crate) fn drain_to_end(&mut self) -> std::io::Result<()> {
        let mut buf = [0u8; 4096];
        loop {
            let bytes_read = self.read(&mut buf)?;
            if bytes_read == 0 {
                return Ok(());
            }
        }
    }
}

impl<R: Read> Read for ChecksumReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.read(buf)?;
        self.hasher.update(&buf[..bytes_read]);
        Ok(bytes_read)
    }
}

pub fn format_checksum(checksum: u32) -> String {
    format!("{:08x}", checksum)
}

/// Name of the manifest file in the data directory
pub const CHECKSUMS_MANIFEST: &str = "checksums.manifest";

/// Get the manifest file path for a data directory
pub fn manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CHECKSUMS_MANIFEST)
}

/// Read checksum from manifest file for a specific data file
/// `file` is relative to `data_dir`.
/// Returns:
/// - Ok(Some(checksum)) if manifest exists and file is listed
/// - Ok(None) if manifest doesn't exist (backwards compat - use default)
/// - Err(FileNotInManifest) if manifest exists but file is not listed
/// - Err(ChecksumParseFailed) if manifest format is invalid
pub(crate) fn read_checksum_from_manifest(
    data_dir: &Path,
    file: &Path,
) -> Result<Option<u32>, PersistenceError> {
    let manifest = manifest_path(data_dir);
    let contents = match fs::read_to_string(&manifest) {
        Ok(contents) => contents,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                "No manifest file found for {}, loading from default",
                file.display()
            );
            return Ok(None);
        }
        Err(e) => return Err(PersistenceError::IoError(e)),
    };

    let relative_path = file.to_string_lossy();

    // Parse manifest: each line is "checksum filename"
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(PersistenceError::ChecksumParseFailed(format!(
                "Invalid manifest line: {}",
                line
            )));
        }
        if parts[1] == relative_path {
            let checksum = u32::from_str_radix(parts[0], 16).map_err(|_| {
                PersistenceError::ChecksumParseFailed(format!("Invalid checksum: {}", parts[0]))
            })?;
            return Ok(Some(checksum));
        }
    }

    // File not in manifest - this means file exists but wasn't in a successful shutdown
    Err(PersistenceError::FileNotInManifest(data_dir.join(file)))
}

/// Write manifest file with all checksums atomically
pub fn write_manifest(data_dir: &Path, checksums: &[(PathBuf, u32)]) -> Result<()> {
    let manifest = manifest_path(data_dir);
    let salt: String = deterministic_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    let tmp_manifest = manifest.with_extension(format!("{salt}.tmp"));

    let mut content = String::new();
    for (path, checksum) in checksums {
        let relative_path = path
            .strip_prefix(data_dir)
            .with_context(|| format!("Persisted file is not under {}", data_dir.display()))?
            .to_string_lossy();
        content.push_str(&format!(
            "{} {}\n",
            format_checksum(*checksum),
            relative_path
        ));
    }

    fs::write(&tmp_manifest, &content).context("Writing manifest temp file")?;
    fs::rename(&tmp_manifest, &manifest).context("Renaming manifest file")?;

    info!("Wrote checksum manifest with {} entries", checksums.len());
    Ok(())
}
