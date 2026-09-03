#![cfg(feature = "fs")]

use std::path::PathBuf;

use alternate_storage::{
    StorageClient,
    fs::{FsClient, FsClientConfig, FsClientError},
};
use tokio::fs;

async fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("alternate-storage-{}-{name}", std::process::id()));
    fs::create_dir_all(&dir).await.unwrap();

    dir
}

#[tokio::test]
async fn rejects_missing_root() {
    let root = temp_dir("missing-root").await.join("does-not-exist");

    assert!(matches!(
        FsClient::create(FsClientConfig::builder().root(&root).build()).await,
        Err(FsClientError::RootNotFound(_))
    ));
}

#[tokio::test]
async fn rejects_non_directory_root() {
    let file = temp_dir("non-dir-root").await.join("file");
    fs::write(&file, b"").await.unwrap();

    assert!(matches!(
        FsClient::create(FsClientConfig::builder().root(&file).build()).await,
        Err(FsClientError::RootNotADirectory(_))
    ));
}

#[tokio::test]
async fn round_trips_without_root() {
    let dir = temp_dir("without-root").await;
    let file = dir.join("object.txt");
    let file_path = file.to_str().unwrap();

    let client = FsClient::create(FsClientConfig::builder().build())
        .await
        .unwrap();

    assert!(!client.exists(file_path).await.unwrap());
    client.put(file_path, b"hello").await.unwrap();

    assert!(client.exists(file_path).await.unwrap());
    assert_eq!(
        client.get(file_path).await.unwrap(),
        Some(b"hello".to_vec())
    );

    client.delete(file_path).await.unwrap();
    assert_eq!(client.get(file_path).await.unwrap(), None);

    fs::remove_dir_all(&dir).await.unwrap();
}

#[tokio::test]
async fn round_trips_with_root() {
    let root = temp_dir("with-root").await;
    let file_path = "nested/object.txt";

    let client = FsClient::create(FsClientConfig::builder().root(&root).build())
        .await
        .unwrap();

    assert!(!client.exists(file_path).await.unwrap());
    client.put(file_path, b"scoped").await.unwrap();

    assert!(client.exists(file_path).await.unwrap());
    assert!(root.join(file_path).is_file());
    assert_eq!(
        client.get(file_path).await.unwrap(),
        Some(b"scoped".to_vec())
    );

    client.delete(file_path).await.unwrap();
    assert_eq!(client.get(file_path).await.unwrap(), None);

    fs::remove_dir_all(&root).await.unwrap();
}

#[tokio::test]
async fn rejects_absolute_paths_with_root() {
    let root = temp_dir("absolute-path").await;
    let client = FsClient::create(FsClientConfig::builder().root(&root).build())
        .await
        .unwrap();

    assert!(matches!(
        client.put("/etc/hostname", b"escaped").await,
        Err(FsClientError::AbsolutePath(p)) if p == "/etc/hostname"
    ),);

    fs::remove_dir_all(&root).await.unwrap();
}
