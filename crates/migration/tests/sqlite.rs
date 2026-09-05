#![cfg(feature = "sqlite")]

use std::sync::atomic::{AtomicU64, Ordering};

use alternate_migration::{
    Migration, MigrationError, SyncMigrationBackend, SyncMigrationRunner,
    sqlite::SqliteMigrationBackend,
};
use jiff::{Span, Timestamp};
use rusqlite::{Connection, OpenFlags};

const METADATA_TABLE: &str = "_alternate_migrations";

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    backend: SqliteMigrationBackend,
    probe: Connection,
}

fn fixture() -> Fixture {
    let name = format!(
        "file:sqlite_migration_test_{}?mode=memory&cache=shared",
        DB_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let conn = Connection::open_with_flags(&name, flags).expect("open backend connection");
    let probe = Connection::open_with_flags(&name, flags).expect("open probe connection");

    Fixture {
        backend: SqliteMigrationBackend::new(conn),
        probe,
    }
}

fn migration(version: u16, name: &str, sql: &str) -> Migration {
    Migration::try_new(&format!("{version:04}_{name}.sql"), sql.to_owned()).unwrap()
}

fn checksum(sql: &str) -> String {
    blake3::hash(sql.as_bytes()).to_hex().to_string()
}

fn table_exists(probe: &Connection, table_name: &str) -> bool {
    probe
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .expect("check table exists")
}

fn table_columns(probe: &Connection, table_name: &str) -> Vec<(String, String, bool, bool)> {
    let mut stmt = probe
        .prepare(&format!("PRAGMA table_info({table_name})"))
        .expect("prepare table_info");

    stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>("name")?,
            row.get::<_, String>("type")?,
            row.get::<_, i64>("notnull")? != 0,
            row.get::<_, i64>("pk")? != 0,
        ))
    })
    .expect("query table_info")
    .collect::<Result<Vec<_>, _>>()
    .expect("read table_info rows")
}

#[test]
fn creates_metadata_table_idempotently() {
    let Fixture { mut backend, probe } = fixture();

    backend.ensure_metadata_table().unwrap();
    backend.ensure_metadata_table().unwrap();

    assert!(table_exists(&probe, METADATA_TABLE));

    assert_eq!(
        table_columns(&probe, METADATA_TABLE),
        [
            ("version".to_owned(), "INTEGER".to_owned(), false, true),
            ("name".to_owned(), "TEXT".to_owned(), true, false),
            ("checksum".to_owned(), "TEXT".to_owned(), true, false),
            ("applied_at".to_owned(), "TEXT".to_owned(), true, false),
        ]
    );

    assert_eq!(backend.load_applied().unwrap(), []);
}

#[test]
fn records_and_loads_migrations_in_version_order() {
    let Fixture { mut backend, .. } = fixture();

    backend.ensure_metadata_table().unwrap();

    let second = migration(2, "second", "SELECT 2;");
    let first = migration(1, "first", "SELECT 1;");

    backend.record(&second).unwrap();
    backend.record(&first).unwrap();

    let applied = backend.load_applied().unwrap();

    assert_eq!(
        applied.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        applied.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(applied[0].checksum, checksum("SELECT 1;"));
    assert_eq!(applied[1].checksum, checksum("SELECT 2;"));

    let now = Timestamp::now();

    for applied in &applied {
        assert!(
            applied.applied_at > now - Span::new().hours(24) && applied.applied_at <= now,
            "applied_at `{}` should be recent",
            applied.applied_at
        );
    }
}

#[test]
fn applies_sql_and_records_metadata() {
    let Fixture { mut backend, probe } = fixture();

    backend.ensure_metadata_table().unwrap();

    let sql = "CREATE TABLE probe_data (id INTEGER); INSERT INTO probe_data (id) VALUES (42);";
    let migration = migration(1, "create_probe", sql);
    backend.apply(&migration).unwrap();

    let count: i64 = probe
        .query_row("SELECT count(*) FROM probe_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);

    let applied = backend.load_applied().unwrap();

    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, 1);
    assert_eq!(applied[0].name, "create_probe");
    assert_eq!(applied[0].checksum, checksum(sql));
}

#[test]
fn rolls_back_failed_migration() {
    let Fixture { mut backend, probe } = fixture();

    backend.ensure_metadata_table().unwrap();

    let migration = migration(
        1,
        "broken",
        "CREATE TABLE rollback_probe (id INTEGER);
         SELECT * FROM definitely_missing_table;",
    );

    let result = backend.apply(&migration);

    assert!(matches!(result, Err(MigrationError::Backend(_))));
    assert!(!table_exists(&probe, "rollback_probe"));
    assert_eq!(backend.load_applied().unwrap(), []);
}

#[test]
fn uses_custom_metadata_table() {
    let Fixture { backend, probe } = fixture();
    let mut backend = SqliteMigrationBackend::with_table_name(backend, "_custom");

    backend.ensure_metadata_table().unwrap();
    backend.record(&migration(1, "first", "SELECT 1;")).unwrap();

    let count: i64 = probe
        .query_row("SELECT count(*) FROM _custom", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);

    assert!(!table_exists(&probe, METADATA_TABLE));

    let applied = backend.load_applied().unwrap();

    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].name, "first");
}

#[test]
fn runner_applies_pending_migrations_and_is_idempotent() {
    let Fixture { backend, probe, .. } = fixture();
    let mut runner = SyncMigrationRunner::new(backend);

    let migrations = vec![
        migration(2, "second", "CREATE TABLE second (id INTEGER);"),
        migration(1, "first", "CREATE TABLE first (id INTEGER);"),
    ];

    runner.migrate_to_latest(migrations.clone()).unwrap();
    runner.migrate_to_latest(migrations).unwrap();

    assert!(table_exists(&probe, "first"));
    assert!(table_exists(&probe, "second"));

    let mut stmt = probe
        .prepare(&format!(
            "SELECT version FROM {METADATA_TABLE} ORDER BY version"
        ))
        .unwrap();
    let versions = stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(versions, [1, 2]);
}

#[test]
fn runner_sync_records_without_applying_sql() {
    let Fixture { backend, probe, .. } = fixture();
    let mut runner = SyncMigrationRunner::new(backend);

    runner
        .record_to_latest(vec![migration(
            1,
            "create_untouched",
            "CREATE TABLE untouched (id INTEGER);",
        )])
        .unwrap();

    assert!(!table_exists(&probe, "untouched"));

    let mut stmt = probe
        .prepare(&format!("SELECT version, name FROM {METADATA_TABLE}"))
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows, [(1, "create_untouched".to_owned())]);
}

#[test]
fn runner_rejects_checksum_mismatch() {
    let Fixture { backend, probe, .. } = fixture();
    let mut runner = SyncMigrationRunner::new(backend);

    runner
        .migrate_to_latest(vec![migration(
            1,
            "first",
            "CREATE TABLE first (id INTEGER);",
        )])
        .unwrap();

    let tampered = vec![migration(1, "first", "SELECT 1;")];
    let result = runner.migrate_to_latest(tampered);

    assert!(matches!(
        result,
        Err(MigrationError::ChecksumMismatch { version: 1, .. })
    ));
    assert!(table_exists(&probe, "first"));
}
