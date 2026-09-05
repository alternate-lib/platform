use std::{fs, path::PathBuf};

use alternate_migration::{GenerateError, Migration, MigrationPrefix, generate};

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
fn sequential_ignores_non_sql_files() {
    let dir = temp_dir("sequential-ignores-non-sql");
    fs::write(dir.join("README.txt"), b"").unwrap();
    fs::write(dir.join("notes"), b"").unwrap();

    let path = generate(&dir, "first", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0001_first.sql"));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn rejects_malformed_sql_files() {
    let dir = temp_dir("rejects-malformed-sql");
    fs::write(dir.join("notes.sql"), b"").unwrap();
    fs::write(dir.join("001_short.sql"), b"").unwrap();
    fs::write(dir.join("00001_long.sql"), b"").unwrap();
    fs::write(dir.join("0000_zero.sql"), b"").unwrap();
    fs::write(dir.join("0001_BadName.sql"), b"").unwrap();
    fs::write(dir.join("65535_five_digits.sql"), b"").unwrap();

    let result = generate(&dir, "first", MigrationPrefix::Sequential);

    assert!(matches!(
        result,
        Err(GenerateError::InvalidMigrationFile(path)) if path.parent() == Some(dir.as_path())
    ));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn skips_timestamp_files_when_generating_sequential() {
    let dir = temp_dir("sequential-skips-timestamps");
    fs::write(dir.join("20250101123045_add_index.sql"), b"").unwrap();

    let path = generate(&dir, "first", MigrationPrefix::Sequential).unwrap();

    assert_eq!(path, dir.join("0001_first.sql"));

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn sequential_overflows_after_9999() {
    let dir = temp_dir("sequential-overflows");
    fs::write(dir.join("9999_final.sql"), b"").unwrap();

    let result = generate(&dir, "beyond", MigrationPrefix::Sequential);

    assert!(matches!(result, Err(GenerateError::SequentialOverflow)));

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
fn rejects_invalid_names() {
    let dir = temp_dir("rejects-invalid-names");

    for name in ["Create Users", "Add-Users", "", "_private", "naïve"] {
        assert!(
            matches!(
                generate(&dir, name, MigrationPrefix::Sequential),
                Err(GenerateError::InvalidName(reported)) if reported == name
            ),
            "expected `{name}` to be rejected"
        );
    }

    assert!(fs::read_dir(&dir).unwrap().next().is_none());

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn rejects_existing_file_without_modifying_it() {
    let dir = temp_dir("rejects-existing-file-atomically");

    for _ in 0..10 {
        let timestamp = jiff::Timestamp::now().strftime("%Y%m%d%H%M%S").to_string();
        let path = dir.join(format!("{timestamp}_create_users.sql"));
        fs::write(&path, b"sentinel").unwrap();

        match generate(&dir, "create_users", MigrationPrefix::Timestamp) {
            Err(GenerateError::AlreadyExists(existing)) if existing == path => {
                assert_eq!(fs::read_to_string(&path).unwrap(), "sentinel");

                fs::remove_dir_all(&dir).unwrap();

                return;
            }
            Ok(created) => {
                fs::remove_file(created).unwrap();
            }
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }

    panic!("failed to observe an AlreadyExists collision after 10 attempts");
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
fn generated_files_round_trip_through_try_new() {
    let dir = temp_dir("generated-round-trip");
    let path = generate(&dir, "create_users", MigrationPrefix::Sequential).unwrap();

    let migration =
        Migration::try_new(path.file_name().unwrap().to_str().unwrap(), String::new()).unwrap();

    assert_eq!(migration.version(), 1);
    assert_eq!(migration.name(), "create_users");

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn migration_error_displays_are_informative() {
    let invalid = Migration::try_new("bad", String::new()).unwrap_err();

    assert_eq!(
        invalid.to_string(),
        "invalid migration filename `bad`: must match `NNNN_name.sql` (sequential, 0001-9999) or `YYYYMMDDHHMMSS_name.sql` (timestamp) with a lowercase snake_case name"
    );
}
