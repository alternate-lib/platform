use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use jiff::Timestamp;

use crate::{
    AppliedMigration, AsyncMigrationBackend, Migration, MigrationError, SyncMigrationBackend,
};

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
    pub(crate) calls: Rc<RefCell<Vec<String>>>,
    pub(crate) applied: Rc<RefCell<Vec<AppliedMigration>>>,
    pub(crate) fail_ensure_metadata_table: Rc<Cell<bool>>,
    pub(crate) fail_load_applied: Rc<Cell<bool>>,
    pub(crate) fail_apply_versions: Rc<RefCell<Vec<u64>>>,
}

impl FakeBackend {
    pub(crate) fn push(&self, call: &str) {
        self.calls.borrow_mut().push(call.to_owned());
    }

    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.borrow().to_owned()
    }
}

impl SyncMigrationBackend for FakeBackend {
    fn ensure_metadata_table(&mut self) -> Result<(), MigrationError> {
        self.push("ensure_metadata_table");

        if self.fail_ensure_metadata_table.get() {
            Err(MigrationError::Backend(anyhow::anyhow!(
                "ensure_metadata_table failed"
            )))
        } else {
            Ok(())
        }
    }

    fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, MigrationError> {
        self.push("load_applied");

        if self.fail_load_applied.get() {
            Err(MigrationError::Backend(anyhow::anyhow!(
                "load_applied failed"
            )))
        } else {
            Ok(self.applied.borrow().clone())
        }
    }

    fn apply(&mut self, migration: &Migration) -> Result<(), MigrationError> {
        self.push(&format!("apply:{}", migration.version()));

        if self
            .fail_apply_versions
            .borrow()
            .contains(&migration.version())
        {
            Err(MigrationError::Backend(anyhow::anyhow!("apply failed")))
        } else {
            Ok(())
        }
    }

    fn record(&mut self, migration: &Migration) -> Result<(), MigrationError> {
        self.push(&format!("record:{}", migration.version()));

        Ok(())
    }
}

impl AsyncMigrationBackend for FakeBackend {
    fn ensure_metadata_table(&self) -> impl Future<Output = Result<(), MigrationError>> {
        self.push("ensure_metadata_table");

        let result = if self.fail_ensure_metadata_table.get() {
            Err(MigrationError::Backend(anyhow::anyhow!(
                "ensure_metadata_table failed"
            )))
        } else {
            Ok(())
        };

        std::future::ready(result)
    }

    fn load_applied(
        &mut self,
    ) -> impl Future<Output = Result<Vec<AppliedMigration>, MigrationError>> {
        self.push("load_applied");

        let result = if self.fail_load_applied.get() {
            Err(MigrationError::Backend(anyhow::anyhow!(
                "load_applied failed"
            )))
        } else {
            Ok(self.applied.borrow().clone())
        };

        std::future::ready(result)
    }

    fn apply(&mut self, migration: &Migration) -> impl Future<Output = Result<(), MigrationError>> {
        self.push(&format!("apply:{}", migration.version()));

        let result = if self
            .fail_apply_versions
            .borrow()
            .contains(&migration.version())
        {
            Err(MigrationError::Backend(anyhow::anyhow!("apply failed")))
        } else {
            Ok(())
        };

        std::future::ready(result)
    }

    fn record(
        &mut self,
        migration: &Migration,
    ) -> impl Future<Output = Result<(), MigrationError>> {
        self.push(&format!("record:{}", migration.version()));

        std::future::ready(Ok(()))
    }
}
