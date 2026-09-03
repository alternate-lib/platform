use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

use jiff::Timestamp;

#[cfg(feature = "postgres")]
pub mod postgres;

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

#[derive(Clone, Debug)]
pub struct MigrationRunner<B> {
    backend: B,
}

impl<B> MigrationRunner<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: MigrationBackend> MigrationRunner<B> {
    pub async fn migrate_to_latest(
        &self,
        migrations: Vec<Migration>,
    ) -> Result<(), MigrationError> {
        self.run(migrations, false).await
    }

    pub async fn sync_to_latest(&self, migrations: Vec<Migration>) -> Result<(), MigrationError> {
        self.run(migrations, true).await
    }

    async fn run(&self, migrations: Vec<Migration>, sync_only: bool) -> Result<(), MigrationError> {
        let migrations = sort_migrations(migrations)?;

        self.backend.ensure_metadata_table().await?;

        let applied = self.backend.load_applied().await?;
        let applied_by_version = applied
            .iter()
            .map(|migration| (migration.version, migration))
            .collect::<BTreeMap<_, _>>();

        let embedded_by_version = migrations
            .iter()
            .map(|migration| (migration.version, migration))
            .collect::<BTreeMap<_, _>>();

        validate_applied_history(&applied_by_version, &embedded_by_version)?;
        let highest_applied_version = applied_by_version.keys().next_back().copied();

        for migration in migrations {
            if applied_by_version.contains_key(&migration.version) {
                continue;
            }

            validate_pending_migration_version(migration.version, highest_applied_version)?;

            if sync_only {
                self.backend.record(&migration).await?;
            } else {
                self.backend.apply(&migration).await?;
            }
        }

        Ok(())
    }
}

fn sort_migrations(mut migrations: Vec<Migration>) -> Result<Vec<Migration>, MigrationError> {
    migrations.sort_unstable_by_key(|migration| migration.version);

    for migrations in migrations.windows(2) {
        let [previous_migration, migration] = migrations else {
            continue;
        };

        if migration.version == previous_migration.version {
            return Err(MigrationError::DuplicateVersion {
                version: migration.version,
                name: migration.name.clone(),
                previous_name: previous_migration.name.clone(),
            });
        }
    }

    Ok(migrations)
}

fn validate_applied_history(
    applied_by_version: &BTreeMap<u64, &AppliedMigration>,
    embedded_by_version: &BTreeMap<u64, &Migration>,
) -> Result<(), MigrationError> {
    for applied_migration in applied_by_version.values() {
        let Some(embedded_migration) = embedded_by_version.get(&applied_migration.version) else {
            return Err(MigrationError::DirtyHistory {
                version: applied_migration.version,
                name: applied_migration.name.clone(),
            });
        };

        if applied_migration.checksum != embedded_migration.checksum {
            return Err(MigrationError::ChecksumMismatch {
                version: applied_migration.version,
                name: embedded_migration.name.clone(),
                expected_checksum: embedded_migration.checksum.clone(),
                actual_checksum: applied_migration.checksum.clone(),
            });
        }
    }

    Ok(())
}

fn validate_pending_migration_version(
    version: u64,
    highest_applied_version: Option<u64>,
) -> Result<(), MigrationError> {
    if let Some(highest_applied_version) = highest_applied_version
        && version < highest_applied_version
    {
        return Err(MigrationError::OutOfOrder {
            version,
            highest_applied: highest_applied_version,
        });
    }

    Ok(())
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
