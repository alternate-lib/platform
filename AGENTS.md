# AGENTS.md

## Overview

A Cargo workspace containing platform/infrastructure adapter crates for application development. Primarily for internal use within the `alternate` organization.

## Layout

```
crates/
├── email/      – Email client abstraction (SMTP)
├── http/       – HTTP client abstraction (reqwest)
├── kv/         – KV store abstraction (Postgres, Redis, SQLite)
├── migration/  – SQL migration abstraction (Postgres, SQLite)
├── queue/      – Message queuing abstraction (Postgres)
└── storage/    – Object storage abstraction (FS, S3)
```
