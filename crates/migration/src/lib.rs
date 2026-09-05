use std::{
    fs, io,
    path::{Path, PathBuf},
};

use jiff::{Timestamp, civil::Date};
pub use runner::*;

#[cfg(feature = "postgres")]
pub mod postgres;
mod plan;
mod runner;
#[cfg(feature = "sqlite")]
pub mod sqlite;

const MAX_SEQUENTIAL_VERSION: u16 = 9999;

pub trait MigrationBackend {
    fn ensure_metadata_table(&self) -> impl Future<Output = Result<(), MigrationError>>;

    fn load_applied(&self) -> impl Future<Output = Result<Vec<AppliedMigration>, MigrationError>>;

    fn apply(&self, migration: &Migration) -> impl Future<Output = Result<(), MigrationError>>;

    fn record(&self, migration: &Migration) -> impl Future<Output = Result<(), MigrationError>>;
}

pub trait SyncMigrationBackend {
    fn ensure_metadata_table(&mut self) -> Result<(), MigrationError>;

    fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, MigrationError>;

    fn apply(&mut self, migration: &Migration) -> Result<(), MigrationError>;

    fn record(&mut self, migration: &Migration) -> Result<(), MigrationError>;
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
        let Some((prefix, name)) = parse_migration_filename(filename) else {
            return Err(MigrationError::InvalidFilename(filename.to_string()));
        };

        let version = match prefix {
            VersionPrefix::Sequential(version) => u64::from(version),
            VersionPrefix::Timestamp(version) => version,
        };

        let checksum = blake3::hash(contents.as_bytes()).to_hex().to_string();

        Ok(Self {
            version,
            name: name.to_owned(),
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
    if !is_valid_migration_name(name) {
        return Err(GenerateError::InvalidName(name.to_owned()));
    }

    fs::create_dir_all(dir)?;

    let version_str = match prefix {
        MigrationPrefix::Timestamp => Timestamp::now().strftime("%Y%m%d%H%M%S").to_string(),
        MigrationPrefix::Sequential => format!("{:04}", next_sequential(dir)?),
    };

    let path = dir.join(format!("{version_str}_{name}.sql"));

    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                GenerateError::AlreadyExists(path.clone())
            } else {
                GenerateError::Io(err)
            }
        })?;

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
        let raw_file_name = entry.file_name();

        if !raw_file_name.as_encoded_bytes().ends_with(b".sql") {
            continue;
        }

        let Some(filename) = raw_file_name.to_str() else {
            return Err(GenerateError::InvalidMigrationFile(entry.path()));
        };

        match parse_migration_filename(filename) {
            Some((VersionPrefix::Sequential(version), _)) => {
                max_version = Some(max_version.map_or(version, |m| m.max(version)));
            }
            Some((VersionPrefix::Timestamp(_), _)) => {}
            None => return Err(GenerateError::InvalidMigrationFile(entry.path())),
        }
    }

    match max_version {
        None => Ok(1),
        Some(MAX_SEQUENTIAL_VERSION) => Err(GenerateError::SequentialOverflow),
        Some(version) => Ok(version + 1),
    }
}

fn parse_migration_filename(filename: &str) -> Option<(VersionPrefix, &str)> {
    let (version_str, name) = filename.strip_suffix(".sql")?.split_once('_')?;

    if !is_valid_migration_name(name) {
        return None;
    }

    if version_str.is_empty() || !version_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    match version_str.len() {
        4 => {
            let version = version_str.parse::<u16>().ok()?;

            if version == 0 {
                return None;
            }

            Some((VersionPrefix::Sequential(version), name))
        }
        14 => {
            let version = version_str.parse::<u64>().ok()?;

            if !is_valid_timestamp_version(version) {
                return None;
            }

            Some((VersionPrefix::Timestamp(version), name))
        }
        _ => None,
    }
}

fn is_valid_migration_name(name: &str) -> bool {
    matches!(name.chars().next(), Some(first) if first.is_ascii_lowercase() || first.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_valid_timestamp_version(version: u64) -> bool {
    let (Ok(year), Ok(month), Ok(day)) = (
        i16::try_from(version / 10_000_000_000),
        i8::try_from(version / 100_000_000 % 100),
        i8::try_from(version / 1_000_000 % 100),
    ) else {
        return false;
    };

    let (Ok(hour), Ok(minute), Ok(second)) = (
        i8::try_from(version / 10_000 % 100),
        i8::try_from(version / 100 % 100),
        i8::try_from(version % 100),
    ) else {
        return false;
    };

    hour <= 23 && minute <= 59 && second <= 59 && Date::new(year, month, day).is_ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VersionPrefix {
    Sequential(u16),
    Timestamp(u64),
}

#[derive(Clone, Copy, Debug)]
pub enum MigrationPrefix {
    Sequential,
    Timestamp,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error(
        "invalid migration filename `{0}`: must match `NNNN_name.sql` (sequential, 0001-{MAX_SEQUENTIAL_VERSION}) or `YYYYMMDDHHMMSS_name.sql` (timestamp) with a lowercase snake_case name"
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

    #[error("duplicate applied migration version {version} in backend history")]
    DuplicateAppliedVersion { version: u64 },

    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("no more sequential versions available (reached {MAX_SEQUENTIAL_VERSION})")]
    SequentialOverflow,

    #[error("migration file already exists at `{0}`")]
    AlreadyExists(PathBuf),

    #[error("invalid migration name `{0}`: must be snake_case")]
    InvalidName(String),

    #[error(
        "invalid migration filename `{0}`: must match `NNNN_name.sql` (sequential, 0001-{MAX_SEQUENTIAL_VERSION}) or `YYYYMMDDHHMMSS_name.sql` (timestamp)"
    )]
    InvalidMigrationFile(PathBuf),

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
            // missing or wrong extension
            "0001_create_users",
            "0001_create_users.txt",
            "0001_create_users.SQL",
            // malformed structure
            "0001create_users.sql",
            "0001_.sql",
            "../x.sql",
            // malformed versions
            "00a1_create_users.sql",
            "001_create_users.sql",
            "00001_create_users.sql",
            "202501011230450_short.sql",
            "0000_zero.sql",
            // invalid names
            "0001_Add_users.sql",
            "0001_add-users.sql",
            "0001_na\u{ef}ve.sql",
            "0001_../../escape.sql",
            // impossible calendar values in timestamp versions
            "20251301123045_impossible_month.sql",
            "20250132123045_impossible_day.sql",
            "20250101250000_impossible_hour.sql",
            "20250101126000_impossible_minute.sql",
            "20250101123061_impossible_second.sql",
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
    fn accepts_valid_filenames() {
        let valid = [
            ("0001_a.sql", 1, "a"),
            ("0001_2fast.sql", 1, "2fast"),
            ("0001_add_users_2.sql", 1, "add_users_2"),
            ("9999_final.sql", 9999, "final"),
            (
                "20250101123045_add_index.sql",
                20_250_101_123_045,
                "add_index",
            ),
        ];

        for (filename, version, name) in valid {
            let migration = Migration::try_new(filename, String::new()).unwrap();

            assert_eq!(migration.version(), version, "version for `{filename}`");
            assert_eq!(migration.name(), name, "name for `{filename}`");
        }
    }
}
