use std::collections::BTreeMap;

use crate::{AppliedMigration, Migration, MigrationBackend, MigrationError};

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
        let mut applied_by_version = BTreeMap::<u64, &AppliedMigration>::new();

        for applied_migration in &applied {
            if applied_by_version
                .insert(applied_migration.version, applied_migration)
                .is_some()
            {
                return Err(MigrationError::DuplicateAppliedVersion {
                    version: applied_migration.version,
                });
            }
        }

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

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeMap,
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

    impl MigrationBackend for FakeBackend {
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
            &self,
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

        fn apply(&self, migration: &Migration) -> impl Future<Output = Result<(), MigrationError>> {
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
            &self,
            migration: &Migration,
        ) -> impl Future<Output = Result<(), MigrationError>> {
            self.push(&format!("record:{}", migration.version()));

            std::future::ready(Ok(()))
        }
    }

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

    #[test]
    fn sort_migrations_orders_by_version() {
        let sorted = sort_migrations(vec![
            migration(3, "c"),
            migration(1, "a"),
            migration(2, "b"),
        ])
        .unwrap();

        assert_eq!(
            sorted.iter().map(Migration::version).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[test]
    fn sort_migrations_accepts_empty_and_single() {
        assert_eq!(sort_migrations(Vec::new()).unwrap(), [] as [Migration; 0]);

        let single = sort_migrations(vec![migration(7, "solo")]).unwrap();

        assert_eq!(
            single.iter().map(Migration::version).collect::<Vec<_>>(),
            [7]
        );
    }

    #[test]
    fn sort_migrations_rejects_duplicate_versions() {
        let result = sort_migrations(vec![migration(1, "a"), migration(1, "b")]);

        assert!(matches!(
            result,
            Err(MigrationError::DuplicateVersion { version: 1, .. })
        ));
    }

    #[test]
    fn validate_applied_history_accepts_empty_history() {
        let migration = migration(1, "a");
        let embedded = BTreeMap::from([(1, &migration)]);

        validate_applied_history(&BTreeMap::new(), &embedded).unwrap();
    }

    #[test]
    fn validate_applied_history_accepts_matching_history() {
        let migration = migration(1, "a");
        let embedded = BTreeMap::from([(1, &migration)]);
        let applied_migration = applied_from(&migration);
        let applied_by_version = BTreeMap::from([(1, &applied_migration)]);

        validate_applied_history(&applied_by_version, &embedded).unwrap();
    }

    #[test]
    fn validate_applied_history_rejects_dirty_history() {
        let migration = migration(1, "a");
        let embedded = BTreeMap::from([(1, &migration)]);
        let applied_migration = applied(2, "legacy", "deadbeef".to_owned());
        let applied_by_version = BTreeMap::from([(2, &applied_migration)]);

        assert!(matches!(
            validate_applied_history(&applied_by_version, &embedded),
            Err(MigrationError::DirtyHistory {
                version: 2,
                name
            }) if name == "legacy"
        ));
    }

    #[test]
    fn validate_applied_history_rejects_checksum_mismatch() {
        let migration = migration(1, "a");
        let embedded = BTreeMap::from([(1, &migration)]);
        let applied_migration = applied(1, "a", "deadbeef".to_owned());
        let applied_by_version = BTreeMap::from([(1, &applied_migration)]);

        assert!(matches!(
            validate_applied_history(&applied_by_version, &embedded),
            Err(MigrationError::ChecksumMismatch {
                version: 1,
                expected_checksum,
                actual_checksum,
                ..
            }) if expected_checksum == migration.checksum && actual_checksum == "deadbeef"
        ));
    }

    #[test]
    fn validate_pending_version_accepts_without_history() {
        validate_pending_migration_version(1, None).unwrap();
    }

    #[test]
    fn validate_pending_version_accepts_equal_and_newer() {
        validate_pending_migration_version(2, Some(2)).unwrap();
        validate_pending_migration_version(3, Some(2)).unwrap();
    }

    #[test]
    fn validate_pending_version_rejects_out_of_order() {
        assert!(matches!(
            validate_pending_migration_version(1, Some(2)),
            Err(MigrationError::OutOfOrder {
                version: 1,
                highest_applied: 2
            })
        ));
    }

    #[tokio::test]
    async fn runner_applies_pending_in_sorted_order() {
        let backend = FakeBackend::default();
        let runner = MigrationRunner::new(backend.clone());

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
        let runner = MigrationRunner::new(backend.clone());

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
        let runner = MigrationRunner::new(backend.clone());

        runner.migrate_to_latest(migrations).await.unwrap();

        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_accepts_empty_migration_list() {
        let backend = FakeBackend::default();
        let runner = MigrationRunner::new(backend.clone());

        runner.migrate_to_latest(Vec::new()).await.unwrap();

        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_duplicates_before_touching_backend() {
        let backend = FakeBackend::default();
        let runner = MigrationRunner::new(backend.clone());

        let result = runner
            .migrate_to_latest(vec![migration(1, "a"), migration(1, "b")])
            .await;

        assert!(matches!(
            result,
            Err(MigrationError::DuplicateVersion { version: 1, .. })
        ));
        assert_eq!(backend.calls(), [] as [&str; 0]);
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
        let runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(
            result,
            Err(MigrationError::DirtyHistory { version: 2, .. })
        ));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_duplicate_applied_versions() {
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![
                applied(1, "a", "deadbeef".to_owned()),
                applied(1, "b", "deadbeef".to_owned()),
            ])),
            ..FakeBackend::default()
        };
        let runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(
            result,
            Err(MigrationError::DuplicateAppliedVersion { version: 1 })
        ));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_checksum_mismatch_before_applying() {
        let migration = migration(1, "a");
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![applied(1, "a", "deadbeef".to_owned())])),
            ..FakeBackend::default()
        };
        let runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration]).await;

        assert!(matches!(
            result,
            Err(MigrationError::ChecksumMismatch { version: 1, .. })
        ));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_rejects_out_of_order_pending() {
        let applied_migration = migration(2, "b");
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![applied_from(&applied_migration)])),
            ..FakeBackend::default()
        };
        let runner = MigrationRunner::new(backend.clone());

        let result = runner
            .migrate_to_latest(vec![
                migration(1, "a"),
                applied_migration,
                migration(3, "c"),
            ])
            .await;

        assert!(matches!(
            result,
            Err(MigrationError::OutOfOrder {
                version: 1,
                highest_applied: 2
            })
        ));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn runner_stops_at_first_failed_apply() {
        let backend = FakeBackend {
            fail_apply_versions: Rc::new(RefCell::new(vec![2])),
            ..FakeBackend::default()
        };
        let runner = MigrationRunner::new(backend.clone());

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
        let runner = MigrationRunner::new(backend.clone());

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
        let runner = MigrationRunner::new(backend.clone());

        let result = runner.migrate_to_latest(vec![migration(1, "a")]).await;

        assert!(matches!(result, Err(MigrationError::Backend(_))));
        assert_eq!(backend.calls(), ["ensure_metadata_table", "load_applied"]);
    }

    #[tokio::test]
    async fn sync_records_pending_without_applying() {
        let backend = FakeBackend::default();
        let runner = MigrationRunner::new(backend.clone());

        runner
            .sync_to_latest(vec![migration(2, "b"), migration(1, "a")])
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
    async fn sync_skips_applied_migrations() {
        let first = migration(1, "a");
        let backend = FakeBackend {
            applied: Rc::new(RefCell::new(vec![applied_from(&first)])),
            ..FakeBackend::default()
        };
        let runner = MigrationRunner::new(backend.clone());

        runner
            .sync_to_latest(vec![first, migration(2, "b")])
            .await
            .unwrap();

        assert_eq!(
            backend.calls(),
            ["ensure_metadata_table", "load_applied", "record:2"]
        );
    }
}
