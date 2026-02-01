use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "czepeku", version, about = "Kemono-based Czepeku map downloader")]
pub struct Cli {
    #[arg(long, global = true, default_value = "patreon", help = "Kemono service name")]
    pub service: String,

    #[arg(long, global = true, default_value = "https://kemono.cr", help = "Kemono base URL")]
    pub base_url: String,

    #[arg(long, global = true, default_value = "cookies.txt", help = "Path to cookies.txt for kemono.cr")]
    pub cookies: Option<PathBuf>,

    #[arg(long, global = true, help = "Path to users_posts.json (default: ./users_posts.json)")]
    pub config: Option<PathBuf>,

    #[arg(long, global = true, help = "Data directory (default: ./.czepeku)")]
    pub data_dir: Option<PathBuf>,

    #[arg(long, global = true, help = "SQLite DB path (default: <data-dir>/index.sqlite)")]
    pub db_path: Option<PathBuf>,

    #[arg(long, global = true, help = "Zip download directory (default: <data-dir>/downloads)")]
    pub download_dir: Option<PathBuf>,

    #[arg(long, global = true, help = "Extracted repository directory (default: <data-dir>/repository)")]
    pub repo_dir: Option<PathBuf>,

    #[arg(long, global = true, default_value = "info", help = "Log level (trace|debug|info|warn|error)")]
    pub log_level: String,

    #[arg(long, global = true, help = "Log file path (default: <data-dir>/czepeku.log)")]
    pub log_file: Option<PathBuf>,

    #[arg(long, global = true, default_value_t = false, help = "Disable log file output")]
    pub no_log_file: bool,

    #[arg(long, global = true, help = "Max total unpacked size in MB (default: unlimited)")]
    pub max_unpacked_mb: Option<u64>,

    #[arg(long, global = true, default_value_t = 15)]
    pub max_retries: u32,

    #[arg(long, global = true, default_value_t = 0.5)]
    pub backoff_factor: f32,

    #[arg(long, global = true, default_value_t = 10)]
    pub max_backoff: u64,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Index(IndexArgs),
    Search(SearchArgs),
    Download(DownloadArgs),
    List(ListArgs),
    Repair(RepairArgs),
    Remove(RemoveArgs),
    Normalize(NormalizeArgs),
}

#[derive(Args, Debug)]
pub struct IndexArgs {
    #[arg(long)]
    pub creators: Vec<String>,

    #[arg(long)]
    pub sets: Vec<String>,

    #[arg(long)]
    pub all_posts: bool,
}

#[derive(Args, Debug)]
pub struct SearchArgs {
    pub query: String,

    #[arg(long, default_value_t = 50)]
    pub limit: usize,
}

#[derive(Args, Debug)]
pub struct DownloadArgs {
    pub query: Option<String>,

    #[arg(long, value_delimiter = ',')]
    pub id: Vec<i64>,

    #[arg(long, default_value_t = false, help = "Download all indexed attachments")]
    pub all: bool,

    #[arg(long)]
    pub keep_zip: bool,

    #[arg(long, default_value_t = 4)]
    pub workers: usize,

    #[arg(long, default_value_t = 1000)]
    pub limit: usize,
}

#[derive(Args, Debug)]
pub struct RepairArgs {
    #[arg(long, default_value_t = true, help = "Redownload missing items (default: true)")]
    pub redownload: bool,

    #[arg(long, default_value_t = false, help = "Remove missing items instead of redownloading")]
    pub remove_missing: bool,

    #[arg(long, default_value_t = false, help = "Preview changes without modifying files or DB")]
    pub dry_run: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum RemoveWhat {
    Extract,
    Zip,
    Both,
}

#[derive(Args, Debug)]
pub struct RemoveArgs {
    pub query: Option<String>,

    #[arg(long, value_delimiter = ',')]
    pub id: Vec<i64>,

    #[arg(long, value_enum, default_value_t = RemoveWhat::Extract)]
    pub what: RemoveWhat,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Args, Debug)]
pub struct NormalizeArgs {
    #[arg(long, default_value_t = false, help = "Preview changes without modifying files")]
    pub dry_run: bool,

    #[arg(long = "no-merge", action = clap::ArgAction::SetFalse, default_value_t = true, help = "Disable merging Part folders")]
    pub merge: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum ListType {
    Downloads,
    Attachments,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[arg(long, value_enum, default_value_t = ListType::Downloads)]
    pub r#type: ListType,

    #[arg(long, help = "Filter by status (processing|completed|failed|merged|removed)")]
    pub status: Option<String>,

    #[arg(long)]
    pub query: Option<String>,

    #[arg(long, default_value_t = 200)]
    pub limit: usize,
}
