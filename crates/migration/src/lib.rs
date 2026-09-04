use std::{
    fs, io,
    path::{Path, PathBuf},
};

use jiff::Timestamp;
pub use runner::*;

#[cfg(feature = "postgres")]
pub mod postgres;
mod runner;

pub trait MigrationBackend {
    fn ensure_metadata_table(&self) -> impl Future<Output = Result<(), MigrationError>>;

    fn load_applied(&self) -> impl Future<Output = Result<Vec<AppliedMigration>, MigrationError>>;

    fn apply(&self, migration: &Migration) -> impl Future<Output = Result<(), MigrationError>>;

    fn record(&self, migration: &Migration) -> impl Future<Output = Result<(), MigrationError>>;
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Migration {
    version: u64,
    name: String,
    pub(crate) sql: String,
    pub(crate) checksum: String,
}

impl Migration {
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn try_new(filename: &str, contents: String) -> Result<Self, MigrationError> {
        if !filename.to_lowercase().ends_with(".sql") {
            return Err(MigrationError::InvalidFilename(filename.to_string()));
        }

        let mut parts = filename.splitn(2, '_');
        let version_str = parts
            .next()
            .ok_or_else(|| MigrationError::InvalidFilename(filename.to_string()))?;
        let name = parts
            .next()
            .ok_or_else(|| MigrationError::InvalidFilename(filename.to_string()))?
            .strip_suffix(".sql")
            .ok_or_else(|| MigrationError::InvalidFilename(filename.to_string()))?
            .to_owned();

        if name.is_empty() {
            return Err(MigrationError::InvalidFilename(filename.to_string()));
        }

        let all_digits = version_str.chars().all(|c| c.is_ascii_digit());

        let version = match version_str.len() {
            4 if all_digits => version_str
                .parse::<u16>()
                .map_err(|_| MigrationError::InvalidFilename(filename.to_string()))?
                .into(),
            14 if all_digits => version_str
                .parse::<u64>()
                .map_err(|_| MigrationError::InvalidFilename(filename.to_string()))?,
            _ => return Err(MigrationError::InvalidFilename(filename.to_string())),
        };

        let checksum = blake3::hash(contents.as_bytes()).to_hex().to_string();

        Ok(Self {
            version,
            name,
            sql: contents,
            checksum,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedMigration {
    pub version: u64,
    pub name: String,
    pub checksum: String,
    pub applied_at: Timestamp,
}

pub fn generate(dir: &Path, name: &str, prefix: MigrationPrefix) -> Result<PathBuf, GenerateError> {
    fs::create_dir_all(dir)?;

    let version_str = match prefix {
        MigrationPrefix::Timestamp => Timestamp::now().strftime("%Y%m%d%H%M%S").to_string(),
        MigrationPrefix::Sequential => {
            let next = next_sequential(dir)?;
            format!("{next:04}")
        }
    };

    let filename = format!("{version_str}_{name}.sql");
    let path = dir.join(&filename);

    if path.exists() {
        return Err(GenerateError::AlreadyExists(path));
    }

    fs::File::create(&path)?;

    Ok(path)
}

fn next_sequential(dir: &Path) -> Result<u16, GenerateError> {
    let mut max_version: Option<u16> = None;

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(1),
        Err(e) => return Err(e.into()),
    };

    for entry in entries {
        let entry = entry?;
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();

        if !filename.ends_with(".sql") {
            continue;
        }

        let Some(version_str) = filename.split('_').next() else {
            continue;
        };

        if version_str.len() == 4
            && version_str.chars().all(|c| c.is_ascii_digit())
            && let Ok(v) = version_str.parse::<u16>()
        {
            max_version = Some(max_version.map_or(v, |max| max.max(v)));
        }
    }

    max_version.map_or(Ok(1), |v| {
        v.checked_add(1).ok_or(GenerateError::SequentialOverflow)
    })
}

#[derive(Clone, Copy, Debug)]
pub enum MigrationPrefix {
    Sequential,
    Timestamp,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error(
        "invalid migration filename `{0}`: must start with a version prefix (4-digit sequential or 14-digit timestamp) followed by an underscore and a non-empty name"
    )]
    InvalidFilename(String),

    #[error("duplicate migration version {version}: `{name}` and `{previous_name}`")]
    DuplicateVersion {
        version: u64,
        name: String,
        previous_name: String,
    },

    #[error("applied migration {version} (`{name}`) is missing from the incoming migration set")]
    DirtyHistory { version: u64, name: String },

    #[error(
        "cannot apply older migration {version} after newer version {highest_applied} is already applied"
    )]
    OutOfOrder { version: u64, highest_applied: u64 },

    #[error(
        "migration checksum mismatch for {version} (`{name}`): expected {expected_checksum}, got {actual_checksum}"
    )]
    ChecksumMismatch {
        version: u64,
        name: String,
        expected_checksum: String,
        actual_checksum: String,
    },

    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("no more sequential versions available (reached 9999)")]
    SequentialOverflow,

    #[error("migration file already exists at `{0}`")]
    AlreadyExists(PathBuf),

    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sequential_version() {
        let migration =
            Migration::try_new("0001_create_users.sql", "CREATE TABLE users;".to_owned()).unwrap();

        assert_eq!(migration.version(), 1);
        assert_eq!(migration.name(), "create_users");
        assert_eq!(
            migration.checksum,
            blake3::hash(b"CREATE TABLE users;").to_hex().to_string()
        );
    }

    #[test]
    fn parses_timestamp_version() {
        let migration =
            Migration::try_new("20250101123045_add_index.sql", "CREATE INDEX;".to_owned()).unwrap();

        assert_eq!(migration.version(), 20_250_101_123_045);
        assert_eq!(migration.name(), "add_index");
    }

    #[test]
    fn checksum_depends_on_contents() {
        let first = Migration::try_new("0001_a.sql", "SELECT 1;".to_owned()).unwrap();
        let second = Migration::try_new("0001_a.sql", "SELECT 2;".to_owned()).unwrap();
        let identical = Migration::try_new("0001_a.sql", "SELECT 1;".to_owned()).unwrap();

        assert_ne!(first.checksum, second.checksum);
        assert_eq!(first.checksum, identical.checksum);
    }

    #[test]
    fn rejects_invalid_filenames() {
        let invalid = [
            "0001_create_users",
            "0001_create_users.txt",
            "0001create_users.sql",
            "0001_.sql",
            "00a1_create_users.sql",
            "001_create_users.sql",
            "00001_create_users.sql",
            "202501011230450_short.sql",
        ];

        for filename in invalid {
            assert!(
                matches!(
                    Migration::try_new(filename, String::new()),
                    Err(MigrationError::InvalidFilename(reported)) if reported == filename
                ),
                "expected `{filename}` to be rejected"
            );
        }
    }

    #[test]
    fn rejects_uppercase_extension() {
        // The extension check is case-insensitive but `strip_suffix(".sql")` is
        // not, so an uppercase extension is always rejected. This locks in the
        // current behavior; revisit if case-insensitive handling is intended.
        assert!(matches!(
            Migration::try_new("0001_create_users.SQL", String::new()),
            Err(MigrationError::InvalidFilename(_))
        ));
    }
}
