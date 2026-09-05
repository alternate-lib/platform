use std::collections::BTreeMap;

use crate::{AppliedMigration, Migration, MigrationError};

pub(crate) fn plan_migrations(
    applied: &[AppliedMigration],
    migrations: Vec<Migration>,
) -> Result<Vec<Migration>, MigrationError> {
    let migrations = sort_migrations(migrations)?;

    let mut applied_by_version = BTreeMap::<u64, &AppliedMigration>::new();

    for applied_migration in applied {
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

    let mut pending = Vec::new();

    for migration in migrations {
        if applied_by_version.contains_key(&migration.version) {
            continue;
        }

        validate_pending_migration_version(migration.version, highest_applied_version)?;
        pending.push(migration);
    }

    Ok(pending)
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

    #[test]
    fn plan_returns_pending_in_execution_order() {
        let first = migration(1, "a");
        let third = migration(3, "c");

        let pending = plan_migrations(
            &[applied_from(&first)],
            vec![third.clone(), first.clone(), migration(2, "b")],
        )
        .unwrap();

        assert_eq!(
            pending.iter().map(Migration::version).collect::<Vec<_>>(),
            [2, 3]
        );
    }

    #[test]
    fn plan_accepts_fully_applied_and_empty_inputs() {
        assert_eq!(
            plan_migrations(&[], Vec::new()).unwrap(),
            [] as [Migration; 0]
        );
        assert_eq!(
            plan_migrations(&[], vec![migration(1, "a"), migration(2, "b")])
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn plan_rejects_duplicate_applied_versions() {
        let result = plan_migrations(
            &[
                applied(1, "a", "deadbeef".to_owned()),
                applied(1, "b", "deadbeef".to_owned()),
            ],
            vec![migration(1, "a")],
        );

        assert!(matches!(
            result,
            Err(MigrationError::DuplicateAppliedVersion { version: 1 })
        ));
    }

    #[test]
    fn plan_rejects_dirty_history() {
        let result = plan_migrations(
            &[applied(2, "legacy", "deadbeef".to_owned())],
            vec![migration(1, "a")],
        );

        assert!(matches!(
            result,
            Err(MigrationError::DirtyHistory { version: 2, .. })
        ));
    }

    #[test]
    fn plan_rejects_checksum_mismatch() {
        let migration = migration(1, "a");

        let result = plan_migrations(
            &[applied(1, "a", "deadbeef".to_owned())],
            vec![migration.clone()],
        );

        assert!(matches!(
            result,
            Err(MigrationError::ChecksumMismatch { version: 1, .. })
        ));
    }

    #[test]
    fn plan_rejects_out_of_order_pending() {
        let applied_migration = migration(2, "b");

        let result = plan_migrations(
            &[applied_from(&applied_migration)],
            vec![migration(1, "a"), applied_migration, migration(3, "c")],
        );

        assert!(matches!(
            result,
            Err(MigrationError::OutOfOrder {
                version: 1,
                highest_applied: 2
            })
        ));
    }
}
