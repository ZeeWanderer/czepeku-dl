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
    pub path: String,
    pub status: String,
    pub local_zip: Option<String>,
    pub extract_path: Option<String>,
    pub sha256: Option<String>,
    pub size: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DownloadRow {
    pub path: String,
    pub status: String,
    pub local_zip: Option<String>,
    pub extract_path: Option<String>,
    pub updated_at: Option<String>,
    pub attachment_id: Option<i64>,
    pub name: Option<String>,
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
            path TEXT PRIMARY KEY,
            attachment_id INTEGER,
            status TEXT NOT NULL,
            local_zip TEXT,
            extract_path TEXT,
            sha256 TEXT,
            size INTEGER,
            updated_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_downloads_sha256 ON downloads(sha256);
        CREATE TABLE IF NOT EXISTS merge_map (
            source_path TEXT PRIMARY KEY,
            merged_path TEXT NOT NULL,
            updated_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_merge_map_merged ON merge_map(merged_path);
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

pub fn get_attachment_by_path(conn: &Connection, path: &str) -> Result<Option<AttachmentRow>> {
    conn.query_row(
        r#"
        SELECT a.id, a.name, a.path, a.post_id, a.sha256, a.size, a.server, p.title, p.published
        FROM attachments a
        LEFT JOIN posts p ON p.id = a.post_id
        WHERE a.path = ?
        "#,
        params![path],
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
    limit: usize,
) -> Result<Vec<DownloadRow>> {
    let mut rows = Vec::new();
    let sql = r#"
        SELECT d.path, d.status, d.local_zip, d.extract_path, d.updated_at, d.attachment_id, a.name
        FROM downloads d
        LEFT JOIN attachments a ON a.path = d.path
        WHERE (?1 IS NULL OR d.status = ?1)
        ORDER BY d.updated_at DESC
        LIMIT ?2
    "#;
    let mut stmt = conn.prepare(sql)?;
    let mut iter = stmt.query(params![status, limit as i64])?;
    while let Some(row) = iter.next()? {
        rows.push(DownloadRow {
            path: row.get(0)?,
            status: row.get(1)?,
            local_zip: row.get(2)?,
            extract_path: row.get(3)?,
            updated_at: row.get(4)?,
            attachment_id: row.get(5)?,
            name: row.get(6)?,
        });
    }
    Ok(rows)
}

pub fn update_download_status(conn: &Connection, path: &str, status: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (path, status, updated_at) VALUES (?, ?, datetime('now'))\
         ON CONFLICT(path) DO UPDATE SET status=excluded.status, updated_at=datetime('now')",
        params![path, status],
    )?;
    Ok(())
}

pub fn update_download_extract_path(
    conn: &Connection,
    old_path: &str,
    new_path: &str,
    status: &str,
) -> Result<usize> {
    let changed = conn.execute(
        "UPDATE downloads SET extract_path = ?, status = ?, updated_at = datetime('now')\
         WHERE extract_path = ?",
        params![new_path, status, old_path],
    )?;
    Ok(changed)
}

pub fn upsert_merge_map(conn: &Connection, source: &str, merged: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO merge_map (source_path, merged_path, updated_at) VALUES (?, ?, datetime('now'))\
         ON CONFLICT(source_path) DO UPDATE SET merged_path=excluded.merged_path, updated_at=datetime('now')",
        params![source, merged],
    )?;
    Ok(())
}

pub fn get_merge_target(conn: &Connection, source: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT merged_path FROM merge_map WHERE source_path = ?",
        params![source],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub fn find_completed_by_sha256(conn: &Connection, sha256: &str) -> Result<Option<DownloadRecord>> {
    conn.query_row(
        "SELECT path, status, local_zip, extract_path, sha256, size FROM downloads\
         WHERE status = 'completed' AND sha256 = ? LIMIT 1",
        params![sha256],
        |row| {
            Ok(DownloadRecord {
                path: row.get(0)?,
                status: row.get(1)?,
                local_zip: row.get(2)?,
                extract_path: row.get(3)?,
                sha256: row.get(4)?,
                size: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn get_download_status(conn: &Connection, path: &str) -> Result<Option<DownloadRecord>> {
    conn.query_row(
        "SELECT path, status, local_zip, extract_path, sha256, size FROM downloads WHERE path = ?",
        params![path],
        |row| {
            Ok(DownloadRecord {
                path: row.get(0)?,
                status: row.get(1)?,
                local_zip: row.get(2)?,
                extract_path: row.get(3)?,
                sha256: row.get(4)?,
                size: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn mark_download_processing(
    conn: &Connection,
    path: &str,
    attachment_id: i64,
    sha256: Option<&str>,
    size: Option<i64>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (path, attachment_id, status, sha256, size, updated_at)\
         VALUES (?, ?, 'processing', ?, ?, datetime('now'))\
         ON CONFLICT(path) DO UPDATE SET status='processing', sha256=excluded.sha256, size=excluded.size, updated_at=datetime('now')",
        params![path, attachment_id, sha256, size],
    )?;
    Ok(())
}

pub fn mark_download_completed(
    conn: &Connection,
    path: &str,
    attachment_id: i64,
    sha256: Option<&str>,
    size: Option<i64>,
    local_zip: Option<&str>,
    extract_path: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (path, attachment_id, status, sha256, size, local_zip, extract_path, updated_at)\
         VALUES (?, ?, 'completed', ?, ?, ?, ?, datetime('now'))\
         ON CONFLICT(path) DO UPDATE SET status='completed', sha256=excluded.sha256, size=excluded.size, local_zip=excluded.local_zip, extract_path=excluded.extract_path, updated_at=datetime('now')",
        params![path, attachment_id, sha256, size, local_zip, extract_path],
    )?;
    Ok(())
}

pub fn mark_download_failed(conn: &Connection, path: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO downloads (path, status, updated_at) VALUES (?, 'failed', datetime('now'))\
         ON CONFLICT(path) DO UPDATE SET status='failed', updated_at=datetime('now')",
        params![path],
    )?;
    Ok(())
}
