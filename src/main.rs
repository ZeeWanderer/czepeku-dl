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
        Commands::DownloadAll(args) => run_download_all(&cli, &paths, args)?,
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
    if let Some(id) = args.id {
        if let Some(row) = db::get_attachment_by_id(&conn, id)? {
            items.push(row);
        } else {
            println!("No attachment with id {}", id);
            return Ok(());
        }
    } else if let Some(query) = args.query.as_deref() {
        items = db::search_attachments(&conn, query, args.limit)?;
        if items.is_empty() {
            println!("No matches found.");
            return Ok(());
        }
    } else {
        return Err(anyhow::anyhow!("Provide --id or a query"));
    }

    let client = kemono::build_client(&cli.base_url, cli.cookies.as_deref())?;
    let options = download::DownloadOptions {
        base_url: cli.base_url.clone(),
        download_dir: paths.download_dir.clone(),
        repo_dir: paths.repo_dir.clone(),
        keep_zip: args.keep_zip,
        max_retries: cli.max_retries,
        backoff_factor: cli.backoff_factor,
        max_backoff: cli.max_backoff,
        workers: args.workers,
    };

    download::download_items(&paths.db_path, client, items, options)?;
    Ok(())
}

fn run_download_all(cli: &Cli, paths: &Paths, args: &cli::DownloadAllArgs) -> Result<()> {
    let conn = db::open_db(&paths.db_path)?;
    let items = db::list_all_attachments(&conn)?;

    if items.is_empty() {
        println!("No indexed attachments. Run index first.");
        return Ok(());
    }

    let client = kemono::build_client(&cli.base_url, cli.cookies.as_deref())?;
    let options = download::DownloadOptions {
        base_url: cli.base_url.clone(),
        download_dir: paths.download_dir.clone(),
        repo_dir: paths.repo_dir.clone(),
        keep_zip: args.keep_zip,
        max_retries: cli.max_retries,
        backoff_factor: cli.backoff_factor,
        max_backoff: cli.max_backoff,
        workers: args.workers,
    };

    download::download_items(&paths.db_path, client, items, options)?;
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
