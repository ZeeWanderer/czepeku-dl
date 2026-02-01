use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AttachmentRow {
    pub id: i64,
    pub name: String,
    pub path: String,
    pub post_id: String,
    pub sha256: Option<String>,
    pub size: Option<i64>,
    pub server: Option<String>,
    pub title: Option<String>,
    pub published: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DownloadRecord {
    pub attachment_id: i64,
    pub status: String,
    pub local_zip: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DownloadRow {
    pub attachment_id: i64,
    pub status: String,
    pub local_zip: Option<String>,
    pub updated_at: Option<String>,
    pub name: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExtractedFileRow {
    pub original_path: String,
    pub size: Option<i64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FoldMapRow {
    pub original_path: String,
    pub repo_path: String,
    pub rule: String,
}

#[derive(Debug, Clone)]
pub struct AttachmentRoot {
    pub repo_root: String,
    pub rule: String,
}

pub fn open_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("Failed to open db {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("Failed to set WAL mode")?;
    init_schema(&conn)?;
    Ok(conn)
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS creators (
            name TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            service TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            service TEXT NOT NULL,
            title TEXT,
            published TEXT
        );
        CREATE TABLE IF NOT EXISTS attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            post_id TEXT NOT NULL,
            name TEXT NOT NULL,
            sha256 TEXT,
            size INTEGER,
            server TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_attachments_name ON attachments(name);
        CREATE INDEX IF NOT EXISTS idx_attachments_sha256 ON attachments(sha256);
        CREATE TABLE IF NOT EXISTS downloads (
            attachment_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            local_zip TEXT,
            updated_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
        CREATE TABLE IF NOT EXISTS extracted_files (
            attachment_id INTEGER NOT NULL,
            original_path TEXT NOT NULL,
            size INTEGER,
            sha256 TEXT,
            PRIMARY KEY (attachment_id, original_path)
        );
        CREATE TABLE IF NOT EXISTS attachment_roots (
            attachment_id INTEGER PRIMARY KEY,
            repo_root TEXT NOT NULL,
            rule TEXT NOT NULL,
            updated_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_attachment_roots_root ON attachment_roots(repo_root);
        CREATE TABLE IF NOT EXISTS merge_groups (
            base_name TEXT PRIMARY KEY,
            merged INTEGER NOT NULL,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS fold_map (
            attachment_id INTEGER NOT NULL,
            original_path TEXT NOT NULL,
            repo_path TEXT NOT NULL,
            rule TEXT NOT NULL,
            updated_at TEXT,
            PRIMARY KEY (attachment_id, original_path),
            UNIQUE (repo_path)
        );
        CREATE INDEX IF NOT EXISTS idx_fold_map_attachment ON fold_map(attachment_id);
        "#,
    )?;
    Ok(())
}

pub fn upsert_creator(conn: &Connection, name: &str, user_id: &str, service: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO creators (name, user_id, service) VALUES (?, ?, ?)\
         ON CONFLICT(name) DO UPDATE SET user_id=excluded.user_id, service=excluded.service",
        params![name, user_id, service],
    )?;
    Ok(())
}

pub fn upsert_post(
    conn: &Connection,
    id: &str,
    user_id: &str,
    service: &str,
    title: Option<&str>,
    published: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO posts (id, user_id, service, title, published) VALUES (?, ?, ?, ?, ?)\
         ON CONFLICT(id) DO UPDATE SET title=excluded.title, published=excluded.published",
        params![id, user_id, service, title, published],
    )?;
    Ok(())
}

pub fn upsert_attachment(
    conn: &Connection,
    post_id: &str,
    name: &str,
    path: &str,
    sha256: Option<&str>,
    size: Option<i64>,
    server: Option<&str>,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO attachments (post_id, name, path, sha256, size, server) VALUES (?, ?, ?, ?, ?, ?)\
         ON CONFLICT(path) DO UPDATE SET post_id=excluded.post_id, name=excluded.name, sha256=excluded.sha256, size=excluded.size, server=excluded.server",
        params![post_id, name, path, sha256, size, server],
    )?;
    let id: i64 = conn.query_row(
        "SELECT id FROM attachments WHERE path = ?",
        params![path],
        |row| row.get(0),
    )?;
    Ok(id)
}

pub fn search_attachments(conn: &Connection, query: &str, limit: usize) -> Result<Vec<AttachmentRow>> {
    let like = format!("%{}%", query);
    let mut stmt = conn.prepare(
        r#"
        SELECT a.id, a.name, a.path, a.post_id, a.sha256, a.size, a.server, p.title, p.published
        FROM attachments a
        LEFT JOIN posts p ON p.id = a.post_id
        WHERE a.name LIKE ? COLLATE NOCASE
        ORDER BY a.name
        LIMIT ?
        "#,
    )?;
    let rows = stmt
        .query_map(params![like, limit as i64], |row| {
            Ok(AttachmentRow {
                id: row.get(0)?,
                name: row.get(1)?,
                path: row.get(2)?,
                post_id: row.get(3)?,
                sha256: row.get(4)?,
                size: row.get(5)?,
                server: row.get(6)?,
                title: row.get(7)?,
                published: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn get_attachment_by_id(conn: &Connection, id: i64) -> Result<Option<AttachmentRow>> {
    conn.query_row(
        r#"
        SELECT a.id, a.name, a.path, a.post_id, a.sha256, a.size, a.server, p.title, p.published
        FROM attachments a
        LEFT JOIN posts p ON p.id = a.post_id
        WHERE a.id = ?
        "#,
        params![id],
        |row| {
            Ok(AttachmentRow {
                id: row.get(0)?,
                name: row.get(1)?,
                path: row.get(2)?,
                post_id: row.get(3)?,
                sha256: row.get(4)?,
                size: row.get(5)?,
                server: row.get(6)?,
                title: row.get(7)?,
                published: row.get(8)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn list_attachments(conn: &Connection, limit: usize) -> Result<Vec<AttachmentRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT a.id, a.name, a.path, a.post_id, a.sha256, a.size, a.server, p.title, p.published
        FROM attachments a
        LEFT JOIN posts p ON p.id = a.post_id
        ORDER BY a.name
        LIMIT ?
        "#,
    )?;
    let rows = stmt
        .query_map(params![limit as i64], |row| {
            Ok(AttachmentRow {
                id: row.get(0)?,
                name: row.get(1)?,
                path: row.get(2)?,
                post_id: row.get(3)?,
                sha256: row.get(4)?,
                size: row.get(5)?,
                server: row.get(6)?,
                title: row.get(7)?,
                published: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_downloads(
    conn: &Connection,
    status: Option<&str>,
    query: Option<&str>,
    limit: usize,
) -> Result<Vec<DownloadRow>> {
    let like = query.map(|q| format!("%{}%", q));
    let mut rows = Vec::new();
    let sql = r#"
        SELECT d.attachment_id, d.status, d.local_zip, d.updated_at, a.name, a.path
        FROM downloads d
        LEFT JOIN attachments a ON a.id = d.attachment_id
        WHERE (?1 IS NULL OR d.status = ?1)
          AND (?2 IS NULL OR a.name LIKE ?2 COLLATE NOCASE)
        ORDER BY d.updated_at DESC
        LIMIT ?3
    "#;
    let mut stmt = conn.prepare(sql)?;
    let mut iter = stmt.query(params![status, like, limit as i64])?;
    while let Some(row) = iter.next()? {
        rows.push(DownloadRow {
            attachment_id: row.get(0)?,
            status: row.get(1)?,
            local_zip: row.get(2)?,
            updated_at: row.get(3)?,
            name: row.get(4)?,
            path: row.get(5)?,
        });
    }
    Ok(rows)
}

pub fn update_download_status(conn: &Connection, attachment_id: i64, status: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (attachment_id, status, updated_at) VALUES (?, ?, datetime('now'))\
         ON CONFLICT(attachment_id) DO UPDATE SET status=excluded.status, updated_at=datetime('now')",
        params![attachment_id, status],
    )?;
    Ok(())
}

pub fn get_download_status(conn: &Connection, attachment_id: i64) -> Result<Option<DownloadRecord>> {
    conn.query_row(
        "SELECT attachment_id, status, local_zip, updated_at FROM downloads WHERE attachment_id = ?",
        params![attachment_id],
        |row| {
            Ok(DownloadRecord {
                attachment_id: row.get(0)?,
                status: row.get(1)?,
                local_zip: row.get(2)?,
                updated_at: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn mark_download_processing(conn: &Connection, attachment_id: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (attachment_id, status, updated_at) VALUES (?, 'processing', datetime('now'))\
         ON CONFLICT(attachment_id) DO UPDATE SET status='processing', updated_at=datetime('now')",
        params![attachment_id],
    )?;
    Ok(())
}

pub fn mark_download_completed(
    conn: &Connection,
    attachment_id: i64,
    local_zip: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (attachment_id, status, local_zip, updated_at)\
         VALUES (?, 'completed', ?, datetime('now'))\
         ON CONFLICT(attachment_id) DO UPDATE SET status='completed', local_zip=excluded.local_zip, updated_at=datetime('now')",
        params![attachment_id, local_zip],
    )?;
    Ok(())
}

pub fn mark_download_failed(conn: &Connection, attachment_id: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (attachment_id, status, updated_at) VALUES (?, 'failed', datetime('now'))\
         ON CONFLICT(attachment_id) DO UPDATE SET status='failed', updated_at=datetime('now')",
        params![attachment_id],
    )?;
    Ok(())
}

pub fn replace_extracted_files(
    conn: &mut Connection,
    attachment_id: i64,
    files: &[ExtractedFileRow],
) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM extracted_files WHERE attachment_id = ?",
        params![attachment_id],
    )?;
    for file in files {
        tx.execute(
            "INSERT INTO extracted_files (attachment_id, original_path, size, sha256) VALUES (?, ?, ?, ?)",
            params![attachment_id, file.original_path, file.size, file.sha256],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn list_extracted_files(conn: &Connection, attachment_id: i64) -> Result<Vec<ExtractedFileRow>> {
    let mut stmt = conn.prepare(
        "SELECT original_path, size, sha256 FROM extracted_files WHERE attachment_id = ? ORDER BY original_path",
    )?;
    let rows = stmt
        .query_map(params![attachment_id], |row| {
            Ok(ExtractedFileRow {
                original_path: row.get(0)?,
                size: row.get(1)?,
                sha256: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_extracted_attachment_ids(conn: &Connection) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT attachment_id FROM extracted_files ORDER BY attachment_id",
    )?;
    let rows = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(rows)
}

pub fn replace_fold_map(conn: &mut Connection, attachment_id: i64, mappings: &[FoldMapRow]) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM fold_map WHERE attachment_id = ?",
        params![attachment_id],
    )?;
    for mapping in mappings {
        tx.execute(
            "INSERT INTO fold_map (attachment_id, original_path, repo_path, rule, updated_at)\
             VALUES (?, ?, ?, ?, datetime('now'))",
            params![attachment_id, mapping.original_path, mapping.repo_path, mapping.rule],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn list_fold_map(conn: &Connection, attachment_id: i64) -> Result<Vec<FoldMapRow>> {
    let mut stmt = conn.prepare(
        "SELECT original_path, repo_path, rule FROM fold_map WHERE attachment_id = ? ORDER BY original_path",
    )?;
    let rows = stmt
        .query_map(params![attachment_id], |row| {
            Ok(FoldMapRow {
                original_path: row.get(0)?,
                repo_path: row.get(1)?,
                rule: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn repo_path_in_use(conn: &Connection, repo_path: &str, attachment_id: i64) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM fold_map WHERE repo_path = ? AND attachment_id != ?",
        params![repo_path, attachment_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn repo_root_in_use(conn: &Connection, repo_root: &str, attachment_id: i64) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM attachment_roots WHERE repo_root = ? AND attachment_id != ?",
        params![repo_root, attachment_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn get_attachment_root(conn: &Connection, attachment_id: i64) -> Result<Option<AttachmentRoot>> {
    conn.query_row(
        "SELECT repo_root, rule FROM attachment_roots WHERE attachment_id = ?",
        params![attachment_id],
        |row| {
            Ok(AttachmentRoot {
                repo_root: row.get(0)?,
                rule: row.get(1)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn set_attachment_root(
    conn: &Connection,
    attachment_id: i64,
    repo_root: &str,
    rule: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO attachment_roots (attachment_id, repo_root, rule, updated_at)\
         VALUES (?, ?, ?, datetime('now'))\
         ON CONFLICT(attachment_id) DO UPDATE SET repo_root=excluded.repo_root, rule=excluded.rule, updated_at=datetime('now')",
        params![attachment_id, repo_root, rule],
    )?;
    Ok(())
}

pub fn get_merge_group(conn: &Connection, base_name: &str) -> Result<Option<bool>> {
    conn.query_row(
        "SELECT merged FROM merge_groups WHERE base_name = ?",
        params![base_name],
        |row| {
            let merged: i64 = row.get(0)?;
            Ok(merged != 0)
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn set_merge_group(conn: &Connection, base_name: &str, merged: bool) -> Result<()> {
    let value = if merged { 1 } else { 0 };
    conn.execute(
        "INSERT INTO merge_groups (base_name, merged, updated_at) VALUES (?, ?, datetime('now'))\
         ON CONFLICT(base_name) DO UPDATE SET merged=excluded.merged, updated_at=datetime('now')",
        params![base_name, value],
    )?;
    Ok(())
}

pub fn clear_attachment_plans(conn: &mut Connection, attachment_id: i64) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM fold_map WHERE attachment_id = ?",
        params![attachment_id],
    )?;
    tx.execute(
        "DELETE FROM extracted_files WHERE attachment_id = ?",
        params![attachment_id],
    )?;
    tx.execute(
        "DELETE FROM attachment_roots WHERE attachment_id = ?",
        params![attachment_id],
    )?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let base = std::env::temp_dir();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            for attempt in 0..20u32 {
                let candidate = base.join(format!(
                    "czepeku-test-{}-{}-{}",
                    prefix,
                    std::process::id(),
                    now + attempt as u128
                ));
                if candidate.exists() {
                    continue;
                }
                fs::create_dir_all(&candidate).expect("create tempdir");
                return Self { path: candidate };
            }
            panic!("Failed to create temp dir");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn search_attachments_uses_fixed_ids() -> Result<()> {
        let dir = TempDir::new("db");
        let db_path = dir.path.join("index.sqlite");
        let conn = open_db(&db_path)?;

        conn.execute(
            "INSERT INTO attachments (id, post_id, name, path) VALUES (?, ?, ?, ?)",
            params![
                327_i64,
                "post-1",
                "Swamp Graveyard Gridded Part 1.zip",
                "/data/327.zip"
            ],
        )?;
        conn.execute(
            "INSERT INTO attachments (id, post_id, name, path) VALUES (?, ?, ?, ?)",
            params![
                328_i64,
                "post-1",
                "Swamp Graveyard Gridless Part 1.zip",
                "/data/328.zip"
            ],
        )?;

        let results = search_attachments(&conn, "Swamp Graveyard", 10)?;
        assert_eq!(results.len(), 2);

        let row = get_attachment_by_id(&conn, 327_i64)?.expect("row");
        assert_eq!(row.name, "Swamp Graveyard Gridded Part 1.zip");
        Ok(())
    }
}
