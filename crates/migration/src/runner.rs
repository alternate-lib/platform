use crate::{AsyncMigrationBackend, Migration, MigrationError, plan::plan_migrations};

#[derive(Clone, Debug)]
pub struct MigrationRunner<B> {
    backend: B,
}

impl<B> MigrationRunner<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: AsyncMigrationBackend> MigrationRunner<B> {
    pub async fn migrate_to_latest(
        &mut self,
        migrations: Vec<Migration>,
    ) -> Result<(), MigrationError> {
        self.run(migrations, false).await
    }

    pub async fn record_to_latest(
        &mut self,
        migrations: Vec<Migration>,
    ) -> Result<(), MigrationError> {
        self.run(migrations, true).await
    }

    async fn run(
        &mut self,
        migrations: Vec<Migration>,
        record_only: bool,
    ) -> Result<(), MigrationError> {
        self.backend.ensure_metadata_table().await?;

        let applied = self.backend.load_applied().await?;
        let pending = plan_migrations(&applied, migrations)?;

        for migration in pending {
            if record_only {
                self.backend.record(&migration).await?;
            } else {
                self.backend.apply(&migration).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    use jiff::Timestamp;

    use super::*;
    use crate::{AppliedMigration, Migration, MigrationError};

    fn migration(version: u16, name: &str) -> Migration {
        Migration::try_new(
            &format!("{version:04}_{name}.sql"),
            format!("-- {version:04} {name}"),
        )
        .unwrap()
    }

    fn applied(version: u64, name: &str, checksum: String) -> AppliedMigration {
        AppliedMigration {
            version,
            name: name.to_owned(),
            checksum,
            applied_at: Timestamp::from_second(0).unwrap(),
        }
    }

    fn applied_from(migration: &Migration) -> AppliedMigration {
        applied(
            migration.version(),
            migration.name(),
            migration.checksum.clone(),
        )
    }

    #[derive(Clone, Default)]
    struct FakeBackend {
        calls: Rc<RefCell<Vec<String>>>,
        applied: Rc<RefCell<Vec<AppliedMigration>>>,
        fail_ensure_metadata_table: Rc<Cell<bool>>,
        fail_load_applied: Rc<Cell<bool>>,
        fail_apply_versions: Rc<RefCell<Vec<u64>>>,
    }

    impl FakeBackend {
        fn push(&self, call: &str) {
            self.calls.borrow_mut().push(call.to_owned());
        }

        fn calls(&self) -> Vec<String> {
            self.calls.borrow().to_owned()
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

        fn apply(
            &mut self,
            migration: &Migration,
        ) -> impl Future<Output = Result<(), MigrationError>> {
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

    #[tokio::test]
    async fn runner_applies_pending_in_sorted_order() {
        let backend = FakeBackend::default();
        let mut runner = MigrationRunner::new(backend.clone());

        runner
            .migrate_to_latest(vec![migration(2, "b"), migration(1, "a")])
            .await
            .unwrap();

        assert_eq!(
            backend.calls(),
            [
                "ensure_metadata_table",
                "load_applied",
                "apply:1",
                "apply:2"
            ]
        );
    }

    #[tokio::test]
    async fn runner_skips_applied_migrations() {
        let first = migration(1, "a");
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![applied_from(&first)])),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        runner
            .migrate_to_latest(vec![first, migration(2, "b")])
            .await
            .unwrap();

        assert_eq!(
            backend.calls(),
            ["ensure_metadata_table", "load_applied", "apply:2"]
        );
    }

    #[tokio::test]
    async fn runner_is_no_op_when_current() {
        let migrations = vec![migration(1, "a"), migration(2, "b")];
        let applied = migrations.iter().map(applied_from).collect::<Vec<_>>();
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(applied)),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        runner.migrate_to_latest(migrations).await.unwrap();

        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_accepts_empty_migration_list() {
        let backend = FakeBackend::default();
        let mut runner = MigrationRunner::new(backend.clone());

        runner.migrate_to_latest(Vec::new()).await.unwrap();

        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_duplicate_versions_without_applying() {
        let backend = FakeBackend::default();
        let mut runner = MigrationRunner::new(backend.clone());

        let result = runner
            .migrate_to_latest(vec![migration(1, "a"), migration(1, "b")])
            .await;

        assert!(matches!(
            result,
            Err(MigrationError::DuplicateVersion { version: 1, .. })
        ));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_dirty_history_before_applying() {
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![applied(
                2,
                "legacy",
                "deadbeef".to_owned(),
            )])),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(
            result,
            Err(MigrationError::DirtyHistory { version: 2, .. })
        ));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_stops_at_first_failed_apply() {
        let backend = FakeBackend {
            fail_apply_versions: Rc::new(RefCell::new(vec![2])),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        let result = runner
            .migrate_to_latest(vec![
                migration(1, "a"),
                migration(2, "b"),
                migration(3, "c"),
            ])
            .await;

        assert!(matches!(result, Err(MigrationError::Backend(_))));
        assert_eq!(
            backend.calls(),
            [
                "ensure_metadata_table",
                "load_applied",
                "apply:1",
                "apply:2"
            ]
        );
    }

    #[tokio::test]
    async fn runner_propagates_ensure_failure() {
        let backend = FakeBackend {
            fail_ensure_metadata_table: Rc::new(Cell::new(true)),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(result, Err(MigrationError::Backend(_))));
        assert_eq!(backend.calls(), ["ensure_metadata_table"]);
    }

    #[tokio::test]
    async fn runner_propagates_load_failure() {
        let backend = FakeBackend {
            fail_load_applied: Rc::new(Cell::new(true)),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(result, Err(MigrationError::Backend(_))));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_records_pending_without_applying() {
        let backend = FakeBackend::default();
        let mut runner = MigrationRunner::new(backend.clone());

        runner
            .record_to_latest(vec![migration(2, "b"), migration(1, "a")])
            .await
            .unwrap();

        assert_eq!(
            backend.calls(),
            [
                "ensure_metadata_table",
                "load_applied",
                "record:1",
                "record:2"
            ]
        );
    }

    #[tokio::test]
    async fn runner_skips_applied_migrations_when_recording() {
        let first = migration(1, "a");
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![applied_from(&first)])),
            ..FakeBackend::default()
        };
        let mut runner = MigrationRunner::new(backend.clone());

        runner
            .record_to_latest(vec![first, migration(2, "b")])
            .await
            .unwrap();

        assert_eq!(
            backend.calls(),
            ["ensure_metadata_table", "load_applied", "record:2"]
        );
    }
}
