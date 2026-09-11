use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

use jiff::Timestamp;

use crate::{AppliedMigration, AsyncMigrationBackend, Migration, SyncMigrationBackend};

pub(crate) fn migration(version: u16, name: &str) -> Migration {
    Migration::try_new(
        &format!("{version:04}_{name}.sql"),
        format!("-- {version:04} {name}"),
    )
    .unwrap()
}

pub(crate) fn applied(version: u64, name: &str, checksum: String) -> AppliedMigration {
    AppliedMigration {
        version,
        name: name.to_owned(),
        checksum,
        applied_at: Timestamp::from_second(0).unwrap(),
    }
}

pub(crate) fn applied_from(migration: &Migration) -> AppliedMigration {
    applied(
        migration.version(),
        migration.name(),
        migration.checksum.clone(),
    )
}

#[derive(Clone, Default)]
pub(crate) struct FakeBackend {
    pub(crate) calls: Arc<RwLock<Vec<String>>>,
    pub(crate) applied: Arc<RwLock<Vec<AppliedMigration>>>,
    pub(crate) fail_ensure_metadata_table: Arc<AtomicBool>,
    pub(crate) fail_load_applied: Arc<AtomicBool>,
    pub(crate) fail_apply_versions: Arc<RwLock<Vec<u64>>>,
}

impl FakeBackend {
    pub(crate) fn push(&self, call: &str) {
        self.calls.write().unwrap().push(call.to_owned());
    }

    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.read().unwrap().to_owned()
    }
}

impl SyncMigrationBackend for FakeBackend {
    type Error = FakeBackendError;

    fn ensure_metadata_table(&mut self) -> Result<(), Self::Error> {
        self.push("ensure_metadata_table");

        if self.fail_ensure_metadata_table.load(Ordering::Relaxed) {
            Err(FakeBackendError::EnsureMetadataTable)
        } else {
            Ok(())
        }
    }

    fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, Self::Error> {
        self.push("load_applied");

        if self.fail_load_applied.load(Ordering::Relaxed) {
            Err(FakeBackendError::LoadApplied)
        } else {
            Ok(self.applied.read().unwrap().to_owned())
        }
    }

    fn apply(&mut self, migration: &Migration) -> Result<(), Self::Error> {
        self.push(&format!("apply:{}", migration.version()));

        if self
            .fail_apply_versions
            .read()
            .unwrap()
            .contains(&migration.version())
        {
            Err(FakeBackendError::Apply)
        } else {
            Ok(())
        }
    }

    fn record(&mut self, migration: &Migration) -> Result<(), Self::Error> {
        self.push(&format!("record:{}", migration.version()));

        Ok(())
    }
}

impl AsyncMigrationBackend for FakeBackend {
    type Error = FakeBackendError;

    async fn ensure_metadata_table(&self) -> Result<(), Self::Error> {
        self.push("ensure_metadata_table");

        if self.fail_ensure_metadata_table.load(Ordering::Relaxed) {
            return Err(FakeBackendError::EnsureMetadataTable);
        }

        Ok(())
    }

    async fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, Self::Error> {
        self.push("load_applied");

        if self.fail_load_applied.load(Ordering::Relaxed) {
            return Err(FakeBackendError::LoadApplied);
        }

        Ok(self.applied.read().unwrap().to_owned())
    }

    async fn apply(&mut self, migration: &Migration) -> Result<(), Self::Error> {
        self.push(&format!("apply:{}", migration.version()));

        if self
            .fail_apply_versions
            .read()
            .unwrap()
            .contains(&migration.version())
        {
            return Err(FakeBackendError::Apply);
        }

        Ok(())
    }

    async fn record(&mut self, migration: &Migration) -> Result<(), Self::Error> {
        self.push(&format!("record:{}", migration.version()));

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FakeBackendError {
    #[error("ensure_metadata_table failed")]
    EnsureMetadataTable,

    #[error("load_applied failed")]
    LoadApplied,

    #[error("apply failed")]
    Apply,
}
