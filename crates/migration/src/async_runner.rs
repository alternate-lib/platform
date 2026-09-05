use crate::{AsyncMigrationBackend, Migration, MigrationError, plan::plan_migrations};

#[derive(Clone, Debug)]
pub struct AsyncMigrationRunner<B> {
    backend: B,
}

impl<B> AsyncMigrationRunner<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: AsyncMigrationBackend> AsyncMigrationRunner<B> {
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

    use super::*;
    use crate::test_utils::{FakeBackend, applied, applied_from, migration};

    #[tokio::test]
    async fn runner_applies_pending_in_sorted_order() {
        let backend = FakeBackend::default();
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

        runner.migrate_to_latest(migrations).await.unwrap();

        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_accepts_empty_migration_list() {
        let backend = FakeBackend::default();
        let mut runner = AsyncMigrationRunner::new(backend.clone());

        runner.migrate_to_latest(Vec::new()).await.unwrap();

        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_duplicate_versions_without_applying() {
        let backend = FakeBackend::default();
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(result, Err(MigrationError::Backend(_))));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_records_pending_without_applying() {
        let backend = FakeBackend::default();
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
        let mut runner = AsyncMigrationRunner::new(backend.clone());

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
