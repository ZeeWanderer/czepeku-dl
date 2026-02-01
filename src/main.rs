mod cli;
mod config;
mod db;
mod download;
mod extract;
mod fsops;
mod fold;
mod kemono;
mod rate_limit;

use anyhow::{Context, Result};
use clap::Parser;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelFilter, SharedLogger, TermLogger, TerminalMode,
    WriteLogger,
};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use walkdir::WalkDir;

use crate::cli::{Cli, Commands};
use crate::fsops::{move_with_fallback, path_from_slash, path_to_slash};
use crate::rate_limit::RateLimiter;

struct Paths {
    data_dir: PathBuf,
    db_path: PathBuf,
    download_dir: PathBuf,
    staging_dir: PathBuf,
    repo_dir: PathBuf,
    config_path: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = resolve_paths(&cli)?;
    let skip_file_log = matches!(cli.command, Commands::Reset(_));
    init_logging(&paths, &cli, skip_file_log)?;
    log::debug!(
        "Paths data_dir={} db_path={} download_dir={} staging_dir={} repo_dir={}",
        paths.data_dir.display(),
        paths.db_path.display(),
        paths.download_dir.display(),
        paths.staging_dir.display(),
        paths.repo_dir.display()
    );

    match &cli.command {
        Commands::Index(args) => run_index(&cli, &paths, args)?,
        Commands::Search(args) => run_search(&paths.db_path, &args.query, args.limit)?,
        Commands::Download(args) => run_download(&cli, &paths, args)?,
        Commands::List(args) => run_list(&paths.db_path, args)?,
        Commands::Repair(args) => run_repair(&cli, &paths, args)?,
        Commands::Remove(args) => run_remove(&cli, &paths, args)?,
        Commands::Normalize(args) => run_normalize(&paths, args)?,
        Commands::Reset(args) => run_reset(&paths, args)?,
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
    let staging_dir = data_dir.join("staging");
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
        staging_dir,
        repo_dir,
        config_path,
    })
}

fn resolve_cookies_path(cli: &Cli) -> Option<PathBuf> {
    if let Some(path) = cli.cookies.clone() {
        if path.as_os_str().is_empty() {
            return Some(PathBuf::from("cookies.txt"));
        }
        return Some(path);
    }
    None
}

fn compute_workers(requested: Option<usize>, items_len: usize) -> usize {
    if let Some(count) = requested {
        return count.max(1);
    }
    if items_len <= 1 {
        return 1;
    }
    let cpu = std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(4);
    let mut workers = if items_len <= cpu { items_len } else { cpu.saturating_mul(2) };
    if workers > 16 {
        workers = 16;
    }
    if items_len > 0 && workers > items_len {
        workers = items_len;
    }
    workers.max(1)
}

fn init_logging(paths: &Paths, cli: &Cli, skip_file_log: bool) -> Result<()> {
    let level = parse_level(&cli.log_level)?;
    let mut loggers: Vec<Box<dyn SharedLogger>> = Vec::new();

    loggers.push(TermLogger::new(
        level,
        ConfigBuilder::new().set_time_format_rfc3339().build(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    ));

    if !cli.no_log_file && !skip_file_log {
        std::fs::create_dir_all(&paths.data_dir)
            .with_context(|| format!("Failed to create {}", paths.data_dir.display()))?;
        let log_path = cli
            .log_file
            .clone()
            .unwrap_or_else(|| paths.data_dir.join("czepeku.log"));
        rotate_log_file(
            &log_path,
            cli.log_max_mb.saturating_mul(1024 * 1024),
            cli.log_backups,
        )?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open log file {}", log_path.display()))?;
        loggers.push(WriteLogger::new(
            LevelFilter::Trace,
            ConfigBuilder::new()
                .set_time_format_rfc3339()
                .set_location_level(LevelFilter::Trace)
                .set_target_level(LevelFilter::Trace)
                .set_thread_level(LevelFilter::Trace)
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

fn rotate_log_file(log_path: &Path, max_bytes: u64, backups: usize) -> Result<()> {
    if max_bytes == 0 || backups == 0 {
        return Ok(());
    }
    let meta = match std::fs::metadata(log_path) {
        Ok(meta) => meta,
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(err).with_context(|| format!("Failed to stat {}", log_path.display()));
        }
    };
    if meta.len() <= max_bytes {
        return Ok(());
    }

    let rotated_path = |idx: usize| PathBuf::from(format!("{}.{}", log_path.display(), idx));

    let oldest = rotated_path(backups);
    if oldest.exists() {
        std::fs::remove_file(&oldest).ok();
    }

    for idx in (1..backups).rev() {
        let src = rotated_path(idx);
        if !src.exists() {
            continue;
        }
        let dest = rotated_path(idx + 1);
        std::fs::rename(&src, &dest).ok();
    }

    let first = rotated_path(1);
    std::fs::rename(log_path, first)
        .with_context(|| format!("Failed to rotate {}", log_path.display()))?;

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

    let cookies_path = resolve_cookies_path(cli);
    let client = kemono::build_client(&cli.base_url, cookies_path.as_deref())?;
    let rate_limiter = cli
        .min_request_interval_ms
        .map(|ms| Arc::new(RateLimiter::new(Duration::from_millis(ms))));
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
            let posts = kemono::list_posts(
                &client,
                &cli.base_url,
                &cli.service,
                &creator.user_id,
                rate_limiter.as_deref(),
            )?;
            log::debug!("Creator {} has {} posts", creator_name, posts.len());
            posts.into_iter().map(|p| p.id).collect::<Vec<_>>()
        } else {
            select_post_ids(&creator.posts, &args.sets)
        };

        for post_id in post_ids {
            log::debug!("Fetching post {}", post_id);
            let envelope = kemono::fetch_post(
                &client,
                &cli.base_url,
                &cli.service,
                &creator.user_id,
                &post_id,
                rate_limiter.as_deref(),
            )
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
                    log::trace!("Attachment {} -> {}", name, path);
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
    log::info!("Index completed attachments={}", attachment_count);
    Ok(())
}

fn run_search(db_path: &Path, query: &str, limit: usize) -> Result<()> {
    let conn = db::open_db(db_path)?;
    log::debug!("Search query='{}' limit={}", query, limit);
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

    let cookies_path = resolve_cookies_path(cli);
    let client = kemono::build_client(&cli.base_url, cookies_path.as_deref())?;
    let rate_limiter = cli
        .min_request_interval_ms
        .map(|ms| Arc::new(RateLimiter::new(Duration::from_millis(ms))));
    let workers = compute_workers(args.workers, items.len());
    log::info!(
        "Download start items={} workers={} force={} keep_zip={}",
        items.len(),
        workers,
        args.force,
        args.keep_zip
    );
    let options = download::DownloadOptions {
        base_url: cli.base_url.clone(),
        download_dir: paths.download_dir.clone(),
        staging_dir: paths.staging_dir.clone(),
        repo_dir: paths.repo_dir.clone(),
        keep_zip: args.keep_zip,
        max_unpacked_bytes: cli.max_unpacked_mb.map(|mb| mb.saturating_mul(1024 * 1024)),
        max_retries: cli.max_retries,
        backoff_factor: cli.backoff_factor,
        max_backoff: cli.max_backoff,
        download_timeout_seconds: cli.download_timeout_seconds,
        workers,
        rate_limiter,
        allow_failures: args.allow_failures,
        force: args.force,
    };

    download::download_items(&paths.db_path, client, items, options)?;
    Ok(())
}

fn run_list(db_path: &Path, args: &cli::ListArgs) -> Result<()> {
    let conn = db::open_db(db_path)?;
    log::debug!(
        "List type={:?} status={:?} query={:?}",
        args.r#type,
        args.status,
        args.query
    );
    match args.r#type {
        cli::ListType::Downloads => {
            let rows = db::list_downloads(
                &conn,
                args.status.as_deref(),
                args.query.as_deref(),
                args.limit,
            )?;
            if rows.is_empty() {
                println!("No downloads found.");
                return Ok(());
            }
            for row in rows {
                let name = row
                    .name
                    .clone()
                    .or(row.path.clone())
                    .unwrap_or_else(|| "-".to_string());
                let updated = row.updated_at.unwrap_or_else(|| "".to_string());
                println!("[{}] {} | {}", row.attachment_id, row.status, name);
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
    let mut conn = db::open_db(&paths.db_path)?;
    log::info!(
        "Repair start dry_run={} redownload={} remove_missing={}",
        args.dry_run,
        args.redownload,
        args.remove_missing
    );
    let downloads = db::list_downloads(&conn, None, None, 10_000)?;

    let mut missing_ids = Vec::new();

    for row in downloads {
        if row.status != "completed" {
            continue;
        }
        let mappings = db::list_fold_map(&conn, row.attachment_id)?;
        if mappings.is_empty() {
            missing_ids.push(row.attachment_id);
            continue;
        }
        let mut missing = false;
        for mapping in mappings {
            let path = paths.repo_dir.join(path_from_slash(&mapping.repo_path));
            if !path.exists() {
                missing = true;
                break;
            }
        }
        if missing {
            missing_ids.push(row.attachment_id);
        }
    }

    log::debug!("Repair missing_ids={}", missing_ids.len());
    if args.dry_run {
        println!("Repair dry-run: missing {}", missing_ids.len());
        return Ok(());
    }

    if args.remove_missing {
        let missing_count = missing_ids.len();
        for id in &missing_ids {
            db::clear_attachment_plans(&mut conn, *id)?;
            db::update_download_status(&conn, *id, "removed")?;
        }
        println!("Marked {} missing items as removed.", missing_count);
        return Ok(());
    }

    if args.redownload {
        let mut items = Vec::new();
        for id in &missing_ids {
            if let Some(att) = db::get_attachment_by_id(&conn, *id)? {
                items.push(att);
            }
        }
        if items.is_empty() {
            println!("No missing items found in attachments table.");
            return Ok(());
        }
        let cookies_path = resolve_cookies_path(cli);
        let client = kemono::build_client(&cli.base_url, cookies_path.as_deref())?;
        let rate_limiter = cli
            .min_request_interval_ms
            .map(|ms| Arc::new(RateLimiter::new(Duration::from_millis(ms))));
        let options = download::DownloadOptions {
            base_url: cli.base_url.clone(),
            download_dir: paths.download_dir.clone(),
            staging_dir: paths.staging_dir.clone(),
            repo_dir: paths.repo_dir.clone(),
            keep_zip: false,
            max_unpacked_bytes: cli.max_unpacked_mb.map(|mb| mb.saturating_mul(1024 * 1024)),
            max_retries: cli.max_retries,
            backoff_factor: cli.backoff_factor,
            max_backoff: cli.max_backoff,
            download_timeout_seconds: cli.download_timeout_seconds,
            workers: compute_workers(None, items.len()),
            rate_limiter,
            allow_failures: false,
            force: true,
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
    let mut conn = db::open_db(&paths.db_path)?;
    log::info!(
        "Remove start query={:?} id_count={} what={:?} dry_run={}",
        args.query,
        args.id.len(),
        args.what,
        args.dry_run
    );
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
        let download = db::get_download_status(&conn, item.id)?;
        let mut deleted_any = false;

        if matches!(args.what, cli::RemoveWhat::Extract | cli::RemoveWhat::Both) {
            let mappings = db::list_fold_map(&conn, item.id)?;
            let repo_paths = mappings
                .iter()
                .map(|row| row.repo_path.clone())
                .collect::<Vec<_>>();
            if args.dry_run {
                for rel in &repo_paths {
                    let path = paths.repo_dir.join(path_from_slash(rel));
                    println!("Would remove {}", path.display());
                }
            } else {
                log::debug!("Removing {} repo paths for {}", repo_paths.len(), item.name);
                let removed_files = fold::remove_repo_files(&paths.repo_dir, &repo_paths)?;
                let _ = removed_files;
                let _ = fold::prune_empty_dirs(&paths.repo_dir, &repo_paths);
                db::clear_attachment_plans(&mut conn, item.id)?;
                deleted_any = true;
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
            db::update_download_status(&conn, item.id, "removed")?;
        }
        removed += 1;
    }

    println!("Processed {} items.", removed);
    log::info!("Remove processed items={}", removed);
    Ok(())
}

fn run_normalize(paths: &Paths, args: &cli::NormalizeArgs) -> Result<()> {
    let mut conn = db::open_db(&paths.db_path)?;
    let ids = db::list_extracted_attachment_ids(&conn)?;
    if ids.is_empty() {
        println!("No extracted files to normalize.");
        return Ok(());
    }

    log::info!("Normalize start attachments={} dry_run={}", ids.len(), args.dry_run);
    let mut touched = 0usize;
    let mut repo_index: Option<HashMap<String, Vec<RepoFileEntry>>> = None;
    let mut used_paths: HashSet<String> = HashSet::new();

    for id in ids {
        let Some(att) = db::get_attachment_by_id(&conn, id)? else {
            continue;
        };
        let extracted = db::list_extracted_files(&conn, id)?;
        if extracted.is_empty() {
            continue;
        }

        let plan = fold::plan_fold(&conn, &paths.repo_dir, id, &att.name, &extracted)?;
        let existing = db::list_fold_map(&conn, id)?;
        let mut existing_map = HashMap::new();
        for row in &existing {
            existing_map.insert(row.original_path.clone(), row.repo_path.clone());
        }

        let mut extracted_map = HashMap::new();
        for row in &extracted {
            extracted_map.insert(row.original_path.clone(), row.clone());
        }

        let mut moves = Vec::new();
        let mut missing_sources = 0usize;
        let mut needs_update = false;

        let mut resolve_source = |original_path: &str,
                                  desired_path: &str,
                                  repo_root: &str|
         -> Result<Option<String>> {
            if let Some(old) = existing_map.get(original_path) {
                let src_path = paths.repo_dir.join(path_from_slash(old));
                if src_path.exists() {
                    return Ok(Some(old.clone()));
                }
            }

            let desired_fs = paths.repo_dir.join(path_from_slash(desired_path));
            if desired_fs.exists() {
                return Ok(Some(desired_path.to_string()));
            }

            let Some(row) = extracted_map.get(original_path) else {
                return Ok(None);
            };
            let (Some(size), Some(sha256)) = (&row.size, &row.sha256) else {
                return Ok(None);
            };
            let key = hash_key(*size, sha256);
            if repo_index.is_none() {
                log::debug!("Building repo hash index for normalize.");
                repo_index = Some(build_repo_hash_index(&paths.repo_dir)?);
            }
            let Some(index) = repo_index.as_mut() else {
                return Ok(None);
            };
            let candidates = index.get_mut(&key);
            let Some(candidates) = candidates else {
                return Ok(None);
            };
            let file_name = Path::new(original_path)
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or("");
            if let Some(picked) =
                select_candidate(candidates, repo_root, file_name, &used_paths)
            {
                used_paths.insert(picked.rel_path.clone());
                return Ok(Some(picked.rel_path));
            }
            Ok(None)
        };

        for mapping in &plan.mappings {
            let source = resolve_source(
                &mapping.original_path,
                &mapping.repo_path,
                &plan.repo_root,
            )?;
            let Some(source) = source else {
                missing_sources += 1;
                continue;
            };
            if source != mapping.repo_path {
                moves.push((source.clone(), mapping.repo_path.clone()));
            }
            if existing_map.get(&mapping.original_path) != Some(&mapping.repo_path) {
                needs_update = true;
            }
        }

        if !needs_update {
            continue;
        }

        touched += 1;
        log::debug!(
            "[{}] {}: moves={} missing={}",
            id,
            att.name,
            moves.len(),
            missing_sources
        );
        if args.dry_run {
            println!(
                "[{}] {}: {} moves, {} missing",
                id,
                att.name,
                moves.len(),
                missing_sources
            );
            continue;
        }

        if missing_sources > 0 {
            println!(
                "[{}] {}: missing {} source files; skipping normalize",
                id, att.name, missing_sources
            );
            continue;
        }

        let mut moved_paths = Vec::new();
        for (from, to) in &moves {
            let src = paths.repo_dir.join(path_from_slash(from));
            let dest = paths.repo_dir.join(path_from_slash(to));
            if !src.exists() {
                println!("Missing source {}", src.display());
                continue;
            }
            if dest.exists() {
                println!("Skip existing {}", dest.display());
                continue;
            }
            move_with_fallback(&src, &dest)?;
            moved_paths.push(from.clone());
        }

        db::replace_fold_map(&mut conn, id, &plan.mappings)?;
        let _ = fold::prune_empty_dirs(&paths.repo_dir, &moved_paths);
    }

    if args.dry_run {
        println!("Normalize dry-run: {} attachments would change.", touched);
    } else {
        println!("Normalized {} attachments.", touched);
    }
    Ok(())
}

fn run_reset(paths: &Paths, args: &cli::ResetArgs) -> Result<()> {
    if !args.yes {
        return Err(anyhow::anyhow!(
            "Reset is destructive. Re-run with --yes to confirm."
        ));
    }
    let data_dir = paths.data_dir.clone();
    if data_dir.as_os_str().is_empty() || data_dir == PathBuf::from("/") {
        return Err(anyhow::anyhow!("Refusing to delete unsafe data directory"));
    }
    if data_dir.exists() {
        match std::fs::remove_dir_all(&data_dir) {
            Ok(()) => {}
            Err(err) => {
                if err.kind() != std::io::ErrorKind::DirectoryNotEmpty {
                    return Err(err).with_context(|| {
                        format!("Failed to remove {}", data_dir.display())
                    });
                }
                for entry in std::fs::read_dir(&data_dir)
                    .with_context(|| format!("Failed to read {}", data_dir.display()))?
                {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        std::fs::remove_dir_all(&path).ok();
                    } else {
                        std::fs::remove_file(&path).ok();
                    }
                }
                std::fs::remove_dir_all(&data_dir)
                    .with_context(|| format!("Failed to remove {}", data_dir.display()))?;
            }
        }
    }
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("Failed to create {}", data_dir.display()))?;
    println!("Reset complete: {}", data_dir.display());
    Ok(())
}

#[derive(Clone)]
struct RepoFileEntry {
    rel_path: String,
    file_name: String,
}

fn hash_key(size: i64, sha256: &str) -> String {
    format!("{}:{}", size, sha256)
}

fn build_repo_hash_index(repo_dir: &Path) -> Result<HashMap<String, Vec<RepoFileEntry>>> {
    let mut index: HashMap<String, Vec<RepoFileEntry>> = HashMap::new();
    let mut files = 0usize;
    for entry in WalkDir::new(repo_dir).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let rel = match path.strip_prefix(repo_dir) {
            Ok(rel) => rel,
            Err(_) => continue,
        };
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        let sha256 = extract::hash_file(path)?;
        let rel_path = path_to_slash(rel);
        let file_name = rel
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("")
            .to_string();
        let key = hash_key(size as i64, &sha256);
        index
            .entry(key)
            .or_default()
            .push(RepoFileEntry { rel_path, file_name });
        files += 1;
    }
    log::debug!("Repo hash index built files={}", files);
    Ok(index)
}

fn select_candidate(
    candidates: &mut Vec<RepoFileEntry>,
    repo_root: &str,
    file_name: &str,
    used_paths: &HashSet<String>,
) -> Option<RepoFileEntry> {
    let root_prefix = if repo_root.is_empty() {
        String::new()
    } else {
        format!("{}/", repo_root)
    };

    let mut pick = |pred: &dyn Fn(&RepoFileEntry) -> bool| -> Option<RepoFileEntry> {
        let pos = candidates
            .iter()
            .position(|entry| !used_paths.contains(&entry.rel_path) && pred(entry))?;
        Some(candidates.remove(pos))
    };

    if !root_prefix.is_empty() {
        if let Some(entry) = pick(&|entry| {
            entry.rel_path.starts_with(&root_prefix) && entry.file_name == file_name
        }) {
            return Some(entry);
        }
    }
    if let Some(entry) = pick(&|entry| entry.file_name == file_name) {
        return Some(entry);
    }
    if !root_prefix.is_empty() {
        if let Some(entry) = pick(&|entry| entry.rel_path.starts_with(&root_prefix)) {
            return Some(entry);
        }
    }
    pick(&|_entry| true)
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
