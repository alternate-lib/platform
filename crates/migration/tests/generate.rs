use std::{fs, path::PathBuf};

use alternate_migration::{Migration, MigrationError, MigrationPrefix, generate};

fn temp_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("alternate-migration-{}-{name}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();

    dir
}

#[test]
fn sequential_starts_at_0001() {
    let dir = temp_dir("sequential-starts-at-0001");
    let path = generate(&dir, "create_users", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0001_create_users.sql"));
    assert!(path.is_file());
    assert_eq!(fs::read_to_string(&path).unwrap(), "");

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn sequential_increments_past_highest_version() {
    let dir = temp_dir("sequential-increments");
    fs::write(dir.join("0001_add_users.sql"), b"").unwrap();
    fs::write(dir.join("0003_add_posts.sql"), b"").unwrap();

    let path = generate(&dir, "add_comments", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0004_add_comments.sql"));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn sequential_ignores_non_sql_and_malformed_files() {
    let dir = temp_dir("sequential-ignores-malformed");
    fs::write(dir.join("README.txt"), b"").unwrap();
    fs::write(dir.join("notes.sql"), b"").unwrap();
    fs::write(dir.join("foo_0001.sql"), b"").unwrap();
    fs::write(dir.join("20250101123045_timestamp.sql"), b"").unwrap();

    let path = generate(&dir, "first", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0001_first.sql"));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn generate_creates_missing_directory() {
    let dir = temp_dir("creates-missing-directory").join("nested");
    let path = generate(&dir, "create_users", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0001_create_users.sql"));
    assert!(path.is_file());

    fs::remove_dir_all(dir.parent().unwrap()).unwrap();
}

#[test]
fn rejects_existing_file() {
    let dir = temp_dir("rejects-existing-file");
    // `next_sequential` only scans 4-digit prefixes, so a collision is only
    // reachable past the scannable maximum: files `9999` and `10000` yield
    // `next = 10000`, which already exists.
    fs::write(dir.join("9999_add_users.sql"), b"").unwrap();
    fs::write(dir.join("10000_create_users.sql"), b"").unwrap();
    let expected = dir.join("10000_create_users.sql");

    let result = generate(&dir, "create_users", MigrationPrefix::Sequential);

    assert!(matches!(
        result,
        Err(alternate_migration::GenerateError::AlreadyExists(path)) if path == expected
    ));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn timestamp_prefix_uses_requested_name() {
    let dir = temp_dir("timestamp-prefix");
    let path = generate(&dir, "add_index", MigrationPrefix::Timestamp).unwrap();

    let filename = path.file_name().unwrap().to_str().unwrap();
    let (version_str, name) = filename
        .strip_suffix(".sql")
        .unwrap()
        .split_once('_')
        .unwrap();

    assert_eq!(version_str.len(), 14);
    assert!(version_str.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(name, "add_index");
    assert_eq!(fs::read_to_string(&path).unwrap(), "");

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn documents_versions_above_9999_are_ignored() {
    // `next_sequential` only scans 4-digit prefixes, so a 5-digit version
    // like `65535` is invisible to it and cannot trigger `SequentialOverflow`
    // (the scannable maximum is 9999, and 9999 + 1 still fits in `u16`).
    // This test documents the current behavior; revisit if 5-digit files
    // should participate in numbering.
    let dir = temp_dir("documents-above-9999");
    fs::write(dir.join("65535_final.sql"), b"").unwrap();

    let path = generate(&dir, "first", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0001_first.sql"));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn documents_sequential_names_past_9999() {
    // `next_sequential` uses `u16::checked_add`, so a directory containing
    // `9999_final.sql` produces `10000_final.sql` instead of returning
    // `SequentialOverflow`. That filename is then rejected by
    // `Migration::try_new`, so generated migrations past 9999 are unusable.
    // This test documents the current behavior; revisit if the boundary
    // should reject generation instead.
    let dir = temp_dir("documents-past-9999");
    fs::write(dir.join("9999_final.sql"), b"").unwrap();

    let path = generate(&dir, "beyond", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("10000_beyond.sql"));
    assert!(matches!(
        Migration::try_new("10000_beyond.sql", String::new()),
        Err(MigrationError::InvalidFilename(_))
    ));

    fs::remove_dir_all(&dir).unwrap();
}
