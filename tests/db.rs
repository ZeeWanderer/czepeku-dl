mod support;

use czepeku::db;
use rusqlite::params;

#[test]
fn search_attachments_uses_fixed_ids() -> anyhow::Result<()> {
    let dir = support::TempDir::new("db");
    let db_path = dir.path().join("index.sqlite");
    let conn = db::open_db(&db_path)?;

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

    let results = db::search_attachments(&conn, "Swamp Graveyard", 10)?;
    assert_eq!(results.len(), 2);

    let row = db::get_attachment_by_id(&conn, 327_i64)?.expect("row");
    assert_eq!(row.name, "Swamp Graveyard Gridded Part 1.zip");
    Ok(())
}

#[test]
fn meta_roundtrip() -> anyhow::Result<()> {
    let dir = support::TempDir::new("meta");
    let db_path = dir.path().join("index.sqlite");
    let conn = db::open_db(&db_path)?;

    assert_eq!(db::get_meta(&conn, "fold_algo")?, None);
    db::set_meta(&conn, "fold_algo", "tree-v1")?;
    assert_eq!(db::get_meta(&conn, "fold_algo")?, Some("tree-v1".to_string()));
    Ok(())
}
