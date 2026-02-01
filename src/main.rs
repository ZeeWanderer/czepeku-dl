mod cli;
mod config;
mod db;
mod download;
mod extract;
mod kemono;

use anyhow::{Context, Result};
use clap::Parser;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelFilter, SharedLogger, TermLogger, TerminalMode,
    WriteLogger,
};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::collections::HashSet;

use crate::cli::{Cli, Commands};

struct Paths {
    data_dir: PathBuf,
    db_path: PathBuf,
    download_dir: PathBuf,
    repo_dir: PathBuf,
    config_path: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = resolve_paths(&cli)?;
    init_logging(&paths, &cli)?;

    match &cli.command {
        Commands::Index(args) => run_index(&cli, &paths, args)?,
        Commands::Search(args) => run_search(&paths.db_path, &args.query, args.limit)?,
        Commands::Download(args) => run_download(&cli, &paths, args)?,
        Commands::List(args) => run_list(&paths.db_path, args)?,
        Commands::Repair(args) => run_repair(&cli, &paths, args)?,
        Commands::Remove(args) => run_remove(&cli, &paths, args)?,
        Commands::Normalize(args) => run_normalize(&paths, args)?,
    }

    Ok(())
}

fn resolve_paths(cli: &Cli) -> Result<Paths> {
    let data_dir = if let Some(path) = cli.data_dir.clone() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".czepeku")
    };

    let db_path = cli
        .db_path
        .clone()
        .unwrap_or_else(|| data_dir.join("index.sqlite"));
    let download_dir = cli
        .download_dir
        .clone()
        .unwrap_or_else(|| data_dir.join("downloads"));
    let repo_dir = cli
        .repo_dir
        .clone()
        .unwrap_or_else(|| data_dir.join("repository"));

    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from("users_posts.json"));

    Ok(Paths {
        data_dir,
        db_path,
        download_dir,
        repo_dir,
        config_path,
    })
}

fn init_logging(paths: &Paths, cli: &Cli) -> Result<()> {
    let level = parse_level(&cli.log_level)?;
    let mut loggers: Vec<Box<dyn SharedLogger>> = Vec::new();

    loggers.push(TermLogger::new(
        level,
        ConfigBuilder::new()
            .set_time_format_rfc3339()
            .build(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    ));

    if !cli.no_log_file {
        std::fs::create_dir_all(&paths.data_dir)
            .with_context(|| format!("Failed to create {}", paths.data_dir.display()))?;
        let log_path = cli
            .log_file
            .clone()
            .unwrap_or_else(|| paths.data_dir.join("czepeku.log"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open log file {}", log_path.display()))?;
        loggers.push(WriteLogger::new(
            level,
            ConfigBuilder::new()
                .set_time_format_rfc3339()
                .build(),
            file,
        ));
    }

    if loggers.is_empty() {
        return Ok(());
    }

    CombinedLogger::init(loggers).context("Failed to init logger")?;
    Ok(())
}

fn parse_level(level: &str) -> Result<LevelFilter> {
    let lvl = match level.to_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" | "warning" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown log level: {} (use trace|debug|info|warn|error)",
                level
            ))
        }
    };
    Ok(lvl)
}

fn run_index(cli: &Cli, paths: &Paths, args: &cli::IndexArgs) -> Result<()> {
    if !paths.config_path.exists() {
        return Err(anyhow::anyhow!(
            "Config file not found: {}",
            paths.config_path.display()
        ));
    }

    log::info!("Index start base_url={}", cli.base_url);
    log::info!("Config file: {}", paths.config_path.display());

    let config = config::load_users_posts(&paths.config_path)?;
    std::fs::create_dir_all(&paths.data_dir)
        .with_context(|| format!("Failed to create {}", paths.data_dir.display()))?;

    let client = kemono::build_client(&cli.base_url, cli.cookies.as_deref())?;
    let conn = db::open_db(&paths.db_path)?;

    let creators = filter_creators(&config, &args.creators);
    if creators.is_empty() {
        println!("No creators matched. Check --creators.");
        return Ok(());
    }

    log::info!("Creators selected: {}", creators.len());

    let mut attachment_count = 0;
    for (creator_name, creator) in creators {
        log::info!("Indexing creator {} ({})", creator_name, creator.user_id);
        db::upsert_creator(&conn, creator_name, &creator.user_id, &cli.service)?;

        let post_ids = if args.all_posts {
            let posts = kemono::list_posts(&client, &cli.base_url, &cli.service, &creator.user_id)?;
            posts.into_iter().map(|p| p.id).collect::<Vec<_>>()
        } else {
            select_post_ids(&creator.posts, &args.sets)
        };

        for post_id in post_ids {
            log::debug!("Fetching post {}", post_id);
            let envelope = kemono::fetch_post(&client, &cli.base_url, &cli.service, &creator.user_id, &post_id)
                .with_context(|| format!("Failed to fetch post {}", post_id))?;

            db::upsert_post(
                &conn,
                &envelope.post.id,
                &creator.user_id,
                &cli.service,
                envelope.post.title.as_deref(),
                envelope.post.published.as_deref(),
            )?;

            let attachments = collect_zip_attachments(&envelope);
            for attachment in attachments {
                if let (Some(name), Some(path)) = (attachment.name.as_deref(), attachment.path.as_deref()) {
                    db::upsert_attachment(
                        &conn,
                        &envelope.post.id,
                        name,
                        path,
                        attachment.sha256.as_deref(),
                        attachment.size,
                        attachment
                            .server
                            .as_deref()
                            .or_else(|| envelope.post.server.as_deref()),
                    )?;
                    attachment_count += 1;
                }
            }
        }
    }

    println!("Indexed {} attachments.", attachment_count);
    Ok(())
}

fn run_search(db_path: &Path, query: &str, limit: usize) -> Result<()> {
    let conn = db::open_db(db_path)?;
    let results = db::search_attachments(&conn, query, limit)?;

    if results.is_empty() {
        println!("No matches found.");
        return Ok(());
    }

    for row in results {
        let title = row.title.unwrap_or_else(|| "(no title)".to_string());
        let published = row.published.unwrap_or_else(|| "".to_string());
        println!("[{}] {} | {} | {}", row.id, row.name, title, published);
    }

    Ok(())
}

fn run_download(cli: &Cli, paths: &Paths, args: &cli::DownloadArgs) -> Result<()> {
    let conn = db::open_db(&paths.db_path)?;

    let mut items = Vec::new();
    let mut seen_paths = HashSet::new();

    if args.all {
        items = db::list_attachments(&conn, args.limit)?;
    } else {
        for id in &args.id {
            if let Some(row) = db::get_attachment_by_id(&conn, *id)? {
                if seen_paths.insert(row.path.clone()) {
                    items.push(row);
                }
            } else {
                println!("No attachment with id {}", id);
            }
        }

        if let Some(query) = args.query.as_deref() {
            let results = db::search_attachments(&conn, query, args.limit)?;
            for row in results {
                if seen_paths.insert(row.path.clone()) {
                    items.push(row);
                }
            }
        }

        if items.is_empty() {
            return Err(anyhow::anyhow!("Provide --id or a query with matches, or use --all"));
        }
    }

    let client = kemono::build_client(&cli.base_url, cli.cookies.as_deref())?;
    let options = download::DownloadOptions {
        base_url: cli.base_url.clone(),
        download_dir: paths.download_dir.clone(),
        repo_dir: paths.repo_dir.clone(),
        keep_zip: args.keep_zip,
        max_unpacked_bytes: cli.max_unpacked_mb.map(|mb| mb.saturating_mul(1024 * 1024)),
        max_retries: cli.max_retries,
        backoff_factor: cli.backoff_factor,
        max_backoff: cli.max_backoff,
        workers: args.workers,
    };

    download::download_items(&paths.db_path, client, items, options)?;
    Ok(())
}

fn run_list(db_path: &Path, args: &cli::ListArgs) -> Result<()> {
    let conn = db::open_db(db_path)?;
    match args.r#type {
        cli::ListType::Downloads => {
            let rows = db::list_downloads(&conn, args.status.as_deref(), args.limit)?;
            if rows.is_empty() {
                println!("No downloads found.");
                return Ok(());
            }
            for row in rows {
                let id = row
                    .attachment_id
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let name = row.name.clone().unwrap_or_else(|| row.path.clone());
                let extract = row.extract_path.unwrap_or_else(|| "".to_string());
                let updated = row.updated_at.unwrap_or_else(|| "".to_string());
                println!("[{}] {} | {} | {}", id, row.status, name, extract);
                if !updated.is_empty() {
                    println!("    updated: {}", updated);
                }
            }
        }
        cli::ListType::Attachments => {
            let rows = if let Some(query) = args.query.as_deref() {
                db::search_attachments(&conn, query, args.limit)?
            } else {
                db::list_attachments(&conn, args.limit)?
            };
            if rows.is_empty() {
                println!("No attachments found.");
                return Ok(());
            }
            for row in rows {
                let title = row.title.clone().unwrap_or_else(|| "(no title)".to_string());
                let published = row.published.clone().unwrap_or_else(|| "".to_string());
                println!("[{}] {} | {} | {}", row.id, row.name, title, published);
            }
        }
    }
    Ok(())
}

fn run_repair(cli: &Cli, paths: &Paths, args: &cli::RepairArgs) -> Result<()> {
    let conn = db::open_db(&paths.db_path)?;
    let downloads = db::list_downloads(&conn, None, 10_000)?;

    let mut missing = Vec::new();

    for row in downloads {
        if let Some(extract_rel) = row.extract_path.clone() {
            let extract_path = paths.repo_dir.join(&extract_rel);
            if !extract_path.exists() {
                if let Some(merged_rel) = db::get_merge_target(&conn, &extract_rel)? {
                    let merged_path = paths.repo_dir.join(&merged_rel);
                    if merged_path.exists() {
                        let _ = db::update_download_extract_path(
                            &conn,
                            &extract_rel,
                            &merged_rel,
                            "merged",
                        )?;
                        continue;
                    }
                }
                missing.push(row);
            }
        } else {
            missing.push(row);
        }
    }

    if args.dry_run {
        println!("Repair dry-run: missing {}", missing.len());
        return Ok(());
    }

    if args.remove_missing {
        let missing_count = missing.len();
        for row in &missing {
            db::update_download_status(&conn, &row.path, "removed")?;
        }
        println!("Marked {} missing items as removed.", missing_count);
        return Ok(());
    }

    if args.redownload {
        let mut items = Vec::new();
        for row in &missing {
            if let Some(att) = db::get_attachment_by_path(&conn, &row.path)? {
                items.push(att);
            }
        }
        if items.is_empty() {
            println!("No missing items found in attachments table.");
            return Ok(());
        }
        let client = kemono::build_client(&cli.base_url, cli.cookies.as_deref())?;
        let options = download::DownloadOptions {
            base_url: cli.base_url.clone(),
            download_dir: paths.download_dir.clone(),
            repo_dir: paths.repo_dir.clone(),
            keep_zip: false,
            max_unpacked_bytes: cli.max_unpacked_mb.map(|mb| mb.saturating_mul(1024 * 1024)),
            max_retries: cli.max_retries,
            backoff_factor: cli.backoff_factor,
            max_backoff: cli.max_backoff,
            workers: 4,
        };
        let item_count = items.len();
        download::download_items(&paths.db_path, client, items, options)?;
        println!("Redownloaded {} items.", item_count);
        return Ok(());
    }

    println!("Repair complete.");
    Ok(())
}

fn run_remove(_cli: &Cli, paths: &Paths, args: &cli::RemoveArgs) -> Result<()> {
    let conn = db::open_db(&paths.db_path)?;
    let mut items = Vec::new();
    let mut seen_paths = HashSet::new();

    for id in &args.id {
        if let Some(row) = db::get_attachment_by_id(&conn, *id)? {
            if seen_paths.insert(row.path.clone()) {
                items.push(row);
            }
        }
    }

    if let Some(query) = args.query.as_deref() {
        let results = db::search_attachments(&conn, query, 10_000)?;
        for row in results {
            if seen_paths.insert(row.path.clone()) {
                items.push(row);
            }
        }
    }

    if items.is_empty() {
        return Err(anyhow::anyhow!("Provide --id or a query with matches"));
    }

    let mut removed = 0usize;
    for item in items {
        let download = db::get_download_status(&conn, &item.path)?;
        let mut deleted_any = false;

        if matches!(args.what, cli::RemoveWhat::Extract | cli::RemoveWhat::Both) {
            if let Some(extract_rel) = download.as_ref().and_then(|d| d.extract_path.clone()) {
                let extract_path = paths.repo_dir.join(&extract_rel);
                if extract_path.exists() {
                    if args.dry_run {
                        println!("Would remove {}", extract_path.display());
                    } else {
                        std::fs::remove_dir_all(&extract_path).ok();
                        deleted_any = true;
                    }
                }
            }
        }

        if matches!(args.what, cli::RemoveWhat::Zip | cli::RemoveWhat::Both) {
            if let Some(zip_path) = download.as_ref().and_then(|d| d.local_zip.clone()) {
                let zip_path = PathBuf::from(zip_path);
                if zip_path.exists() {
                    if args.dry_run {
                        println!("Would remove {}", zip_path.display());
                    } else {
                        std::fs::remove_file(&zip_path).ok();
                        deleted_any = true;
                    }
                }
            }
        }

        if !args.dry_run && deleted_any {
            db::update_download_status(&conn, &item.path, "removed")?;
        }
        removed += 1;
    }

    println!("Processed {} items.", removed);
    Ok(())
}

fn run_normalize(paths: &Paths, args: &cli::NormalizeArgs) -> Result<()> {
    let conn = db::open_db(&paths.db_path)?;
    let flattened = extract::normalize_repo_flatten(&paths.repo_dir, args.dry_run)?;
    let mut merged = 0usize;

    if args.merge {
        let groups = extract::find_part_groups(&paths.repo_dir)?;
        for group in groups {
            let merged_ok = extract::merge_part_group(&group, args.dry_run)?;
            if merged_ok {
                merged += 1;
                let base_rel = group
                    .base_path
                    .strip_prefix(&paths.repo_dir)
                    .unwrap_or(&group.base_path)
                    .to_string_lossy()
                    .to_string();
                for part in &group.part_paths {
                    let part_rel = part
                        .strip_prefix(&paths.repo_dir)
                        .unwrap_or(part)
                        .to_string_lossy()
                        .to_string();
                    if !args.dry_run {
                        db::upsert_merge_map(&conn, &part_rel, &base_rel)?;
                        let _ = db::update_download_extract_path(&conn, &part_rel, &base_rel, "merged")?;
                    }
                }
            }
        }
    }

    if args.dry_run {
        println!(
            "Normalize dry-run: {} folders would be flattened, {} part groups would be merged.",
            flattened, merged
        );
    } else {
        println!("Normalized {} folders, merged {} part groups.", flattened, merged);
    }
    Ok(())
}

fn filter_creators<'a>(
    config: &'a HashMap<String, config::CreatorConfig>,
    selected: &[String],
) -> Vec<(&'a str, &'a config::CreatorConfig)> {
    if selected.is_empty() {
        return config.iter().map(|(k, v)| (k.as_str(), v)).collect();
    }
    config
        .iter()
        .filter(|(name, _)| selected.contains(name))
        .map(|(k, v)| (k.as_str(), v))
        .collect()
}

fn select_post_ids(posts: &HashMap<String, String>, sets: &[String]) -> Vec<String> {
    if sets.is_empty() {
        return posts.values().cloned().collect();
    }
    sets.iter()
        .filter_map(|set| posts.get(set).cloned())
        .collect()
}

fn collect_zip_attachments(envelope: &kemono::PostEnvelope) -> Vec<kemono::Attachment> {
    let mut out = Vec::new();
    let mut seen_paths = HashSet::new();

    if let Some(attachments) = envelope.attachments.clone() {
        for attachment in attachments {
            if is_zip_name(attachment.name.as_deref()) {
                if let Some(path) = attachment.path.as_deref() {
                    if seen_paths.insert(path.to_string()) {
                        out.push(attachment);
                    }
                } else {
                    out.push(attachment);
                }
            }
        }
    }

    if let Some(file) = envelope.post.file.clone() {
        if is_zip_name(file.name.as_deref()) {
            if let Some(path) = file.path.as_deref() {
                if seen_paths.insert(path.to_string()) {
                    out.push(file);
                }
            } else {
                out.push(file);
            }
        }
    }

    if let Some(attachments) = envelope.post.attachments.clone() {
        for attachment in attachments {
            if is_zip_name(attachment.name.as_deref()) {
                if let Some(path) = attachment.path.as_deref() {
                    if seen_paths.insert(path.to_string()) {
                        out.push(attachment);
                    }
                } else {
                    out.push(attachment);
                }
            }
        }
    }

    out
}

fn is_zip_name(name: Option<&str>) -> bool {
    name.map(|n| n.to_lowercase().ends_with(".zip"))
        .unwrap_or(false)
}
