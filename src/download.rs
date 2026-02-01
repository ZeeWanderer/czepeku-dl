use crate::db::{self, AttachmentRow};
use crate::extract;
use crate::fold;
use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info, trace, warn};
use reqwest::blocking::Client;
use reqwest::header::RANGE;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::rate_limit::RateLimiter;

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub base_url: String,
    pub download_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub repo_dir: PathBuf,
    pub keep_zip: bool,
    pub max_unpacked_bytes: Option<u64>,
    pub max_retries: u32,
    pub backoff_factor: f32,
    pub max_backoff: u64,
    pub download_timeout_seconds: u64,
    pub workers: usize,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub allow_failures: bool,
    pub force: bool,
}

pub fn download_items(
    db_path: &Path,
    client: Client,
    items: Vec<AttachmentRow>,
    options: DownloadOptions,
) -> Result<()> {
    if items.is_empty() {
        println!("No matching attachments to download.");
        return Ok(());
    }

    info!(
        "Download queue size={} workers={} force={}",
        items.len(),
        options.workers,
        options.force
    );

    fs::create_dir_all(&options.download_dir)
        .with_context(|| format!("Failed to create {}", options.download_dir.display()))?;
    fs::create_dir_all(&options.staging_dir)
        .with_context(|| format!("Failed to create {}", options.staging_dir.display()))?;
    fs::create_dir_all(&options.repo_dir)
        .with_context(|| format!("Failed to create {}", options.repo_dir.display()))?;

    let total = items.len();
    let queue = Arc::new(Mutex::new(items));
    let mut handles = Vec::new();
    let workers = options.workers.max(1);
    let mp = MultiProgress::new();
    let overall = mp.add(ProgressBar::new(total as u64));
    overall.set_style(
        ProgressStyle::with_template("{spinner} {pos}/{len} {bar:40.cyan/blue} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    overall.enable_steady_tick(Duration::from_millis(100));

    let worker_style = ProgressStyle::with_template(
        "{prefix} {bar:30.cyan/blue} {bytes}/{total_bytes} {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar());

    let mut worker_bars = Vec::new();
    for i in 0..workers {
        let pb = mp.add(ProgressBar::new(0));
        pb.set_style(worker_style.clone());
        pb.set_prefix(format!("#{}", i + 1));
        worker_bars.push(pb);
    }

    let failures: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let completed = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));

    for worker_id in 0..workers {
        let queue = Arc::clone(&queue);
        let client = client.clone();
        let options = options.clone();
        let db_path = db_path.to_path_buf();
        let overall = overall.clone();
        let worker_bar = worker_bars[worker_id].clone();
        let failures = Arc::clone(&failures);
        let completed = Arc::clone(&completed);
        let skipped = Arc::clone(&skipped);
        let failed = Arc::clone(&failed);

        handles.push(thread::spawn(move || {
            debug!("Worker {} started", worker_id + 1);
            loop {
                let item = {
                    let mut guard = queue.lock().unwrap();
                    guard.pop()
                };

                let Some(item) = item else { break; };
                match process_item(&db_path, &client, &options, &item, worker_id, &worker_bar) {
                    Ok(ProcessOutcome::Completed) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(ProcessOutcome::Skipped) => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(ProcessOutcome::Failed(err)) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failures.lock().unwrap().push((item.name.clone(), err));
                    }
                    Err(err) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failures.lock().unwrap().push((item.name.clone(), err.to_string()));
                    }
                }
                overall.inc(1);
            }
            worker_bar.finish_and_clear();
            debug!("Worker {} finished", worker_id + 1);
        }));
    }

    for handle in handles {
        let _ = handle.join();
    }

    overall.finish_with_message("done");

    let failed_count = failed.load(Ordering::Relaxed);
    let skipped_count = skipped.load(Ordering::Relaxed);
    let completed_count = completed.load(Ordering::Relaxed);

    if failed_count > 0 {
        eprintln!(
            "Download summary: {} completed, {} skipped, {} failed",
            completed_count, skipped_count, failed_count
        );
        let failures = failures.lock().unwrap();
        for (name, err) in failures.iter() {
            eprintln!("  - {} ({})", name, err);
        }
        if options.allow_failures {
            return Ok(());
        }
        return Err(anyhow::anyhow!("{} downloads failed", failed_count));
    }

    println!(
        "Download summary: {} completed, {} skipped, {} failed",
        completed_count, skipped_count, failed_count
    );
    info!(
        "Download summary completed={} skipped={} failed={}",
        completed_count, skipped_count, failed_count
    );
    Ok(())
}

enum ProcessOutcome {
    Completed,
    Skipped,
    Failed(String),
}

fn process_item(
    db_path: &Path,
    client: &Client,
    options: &DownloadOptions,
    item: &AttachmentRow,
    worker_id: usize,
    progress: &ProgressBar,
) -> Result<ProcessOutcome> {
    let mut conn = db::open_db(db_path)?;

    if !options.force {
        if let Some(existing) = db::get_download_status(&conn, item.id)? {
            if existing.status == "completed" {
                println!("Skip (done): {}", item.name);
                debug!("Skip completed id={} name={}", item.id, item.name);
                return Ok(ProcessOutcome::Skipped);
            }
        }
    }

    db::mark_download_processing(&conn, item.id)?;

    let url = build_download_url(options, item);
    let zip_path = choose_zip_path(&options.download_dir, &item.name, &item.path);
    let staging_root = options.staging_dir.join(item.id.to_string());
    let temp_path = zip_path.with_extension("part");
    debug!(
        "Download start id={} name={} url={} zip={} staging={}",
        item.id,
        item.name,
        url,
        zip_path.display(),
        staging_root.display()
    );

    if let Err(err) = heal_partial_download(
        &conn,
        item.id,
        &zip_path,
        &temp_path,
        options.download_timeout_seconds,
    ) {
        warn!("Heal failed id={} name={} err={}", item.id, item.name, err);
        db::mark_download_failed(&conn, item.id)?;
        return Ok(ProcessOutcome::Failed(err.to_string()));
    }

    progress.reset();
    progress.set_length(0);
    progress.set_position(0);
    progress.set_message(format!("[{}] {}", worker_id + 1, item.name));

    let download_result = download_with_resume(
        client,
        &url,
        &zip_path,
        options.max_retries,
        options.backoff_factor,
        options.max_backoff,
        options.download_timeout_seconds,
        &progress,
        options.rate_limiter.as_deref(),
    );

    match download_result {
        Ok(()) => {
            progress.finish_with_message(format!("Downloaded {}", item.name));
            debug!("Download completed id={} name={}", item.id, item.name);
            if staging_root.exists() {
                fs::remove_dir_all(&staging_root).ok();
            }
            fs::create_dir_all(&staging_root).with_context(|| {
                format!("Failed to create {}", staging_root.display())
            })?;

            if let Err(err) = extract::extract_zip_to_dir(
                &zip_path,
                &staging_root,
                options.keep_zip,
                options.max_unpacked_bytes,
            ) {
                warn!("Extract failed id={} name={} err={}", item.id, item.name, err);
                db::mark_download_failed(&conn, item.id)?;
                return Ok(ProcessOutcome::Failed(err.to_string()));
            }

            let indexed = match extract::index_extracted_files(&staging_root) {
                Ok(files) => files,
                Err(err) => {
                    db::mark_download_failed(&conn, item.id)?;
                    return Ok(ProcessOutcome::Failed(err.to_string()));
                }
            };
            debug!(
                "Indexed {} files id={} name={}",
                indexed.len(),
                item.id,
                item.name
            );

            let extracted_files = indexed
                .iter()
                .map(|file| db::ExtractedFileRow {
                    original_path: file.rel_path.clone(),
                    size: Some(file.size as i64),
                    sha256: Some(file.sha256.clone()),
                })
                .collect::<Vec<_>>();

            if let Err(err) = db::replace_extracted_files(&mut conn, item.id, &extracted_files) {
                db::mark_download_failed(&conn, item.id)?;
                return Ok(ProcessOutcome::Failed(err.to_string()));
            }

            let plan = match fold::plan_fold(
                &conn,
                &options.repo_dir,
                item.id,
                &item.name,
                &extracted_files,
            ) {
                Ok(plan) => plan,
                Err(err) => {
                    db::mark_download_failed(&conn, item.id)?;
                    return Ok(ProcessOutcome::Failed(err.to_string()));
                }
            };
            debug!(
                "Fold plan id={} name={} root={} mappings={}",
                item.id,
                item.name,
                plan.repo_root,
                plan.mappings.len()
            );

            if options.force {
                if let Ok(existing) = db::list_fold_map(&conn, item.id) {
                    let paths = existing
                        .into_iter()
                        .map(|row| row.repo_path)
                        .collect::<Vec<_>>();
                    debug!(
                        "Force remove {} existing repo paths id={}",
                        paths.len(),
                        item.id
                    );
                    if let Err(err) = fold::remove_repo_files(&options.repo_dir, &paths) {
                        db::mark_download_failed(&conn, item.id)?;
                        return Ok(ProcessOutcome::Failed(err.to_string()));
                    }
                    let _ = fold::prune_empty_dirs(&options.repo_dir, &paths);
                }
            }

            if let Err(err) = fold::apply_plan(&staging_root, &options.repo_dir, &plan, options.force) {
                warn!("Apply plan failed id={} name={} err={}", item.id, item.name, err);
                db::mark_download_failed(&conn, item.id)?;
                return Ok(ProcessOutcome::Failed(err.to_string()));
            }

            if let Err(err) = db::replace_fold_map(&mut conn, item.id, &plan.mappings) {
                db::mark_download_failed(&conn, item.id)?;
                return Ok(ProcessOutcome::Failed(err.to_string()));
            }

            let zip_record = zip_path.to_string_lossy().to_string();
            db::mark_download_completed(&conn, item.id, Some(&zip_record))?;

            fs::remove_dir_all(&staging_root).ok();
            println!("Done: {}", item.name);
            info!("Done id={} name={}", item.id, item.name);
            Ok(ProcessOutcome::Completed)
        }
        Err(err) => {
            progress.finish_with_message(format!("Failed {}", item.name));
            warn!("Download failed id={} name={} err={}", item.id, item.name, err);
            db::mark_download_failed(&conn, item.id)?;
            Ok(ProcessOutcome::Failed(err.to_string()))
        }
    }
}

fn build_download_url(options: &DownloadOptions, item: &AttachmentRow) -> String {
    let base = item
        .server
        .as_deref()
        .unwrap_or(&options.base_url)
        .trim_end_matches('/');
    format!("{}/data{}", base, item.path)
}

fn choose_zip_path(download_dir: &Path, name: &str, path: &str) -> PathBuf {
    let safe_name = sanitize_filename(name);
    let mut candidate = download_dir.join(&safe_name);

    if candidate.exists() {
        let suffix = path
            .split('/')
            .last()
            .and_then(|v| v.split('.').next())
            .unwrap_or("file");
        let stem = Path::new(&safe_name)
            .file_stem()
            .and_then(|v| v.to_str())
            .unwrap_or("archive");
        let ext = Path::new(&safe_name)
            .extension()
            .and_then(|v| v.to_str())
            .unwrap_or("zip");
        let new_name = format!("{}_{}.{}", stem, &suffix[..suffix.len().min(8)], ext);
        candidate = download_dir.join(new_name);
        debug!(
            "Zip name collision, using {}",
            candidate.file_name().and_then(|v| v.to_str()).unwrap_or("archive.zip")
        );
    }

    candidate
}

fn sanitize_filename(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' ) {
            out.push('-');
        } else if ch.is_control() {
            out.push('-');
        } else {
            out.push(ch);
        }
    }
    if out.trim().is_empty() {
        "archive.zip".to_string()
    } else {
        out
    }
}

fn download_with_resume(
    client: &Client,
    url: &str,
    dest_path: &Path,
    max_retries: u32,
    backoff_factor: f32,
    max_backoff: u64,
    download_timeout_seconds: u64,
    progress: &ProgressBar,
    rate_limiter: Option<&RateLimiter>,
) -> Result<()> {
    let temp_path = dest_path.with_extension("part");
    let mut downloaded = temp_path.metadata().map(|m| m.len()).unwrap_or(0);

    let mut attempt = 0;
    while attempt < max_retries {
        attempt += 1;
        progress.set_message(format!("Downloading (attempt {})", attempt));
        debug!(
            "Request {} attempt={} range_start={}",
            url,
            attempt,
            downloaded
        );

        if let Some(limiter) = rate_limiter {
            limiter.wait();
        }

        let mut request = client.get(url);
        if download_timeout_seconds > 0 {
            request = request.timeout(Duration::from_secs(download_timeout_seconds));
        }
        if downloaded > 0 {
            request = request.header(RANGE, format!("bytes={}-", downloaded));
        }

        let response = request.send();
        let mut response = match response {
            Ok(resp) => resp,
            Err(err) => {
                retry_delay(attempt, max_retries, backoff_factor, max_backoff, progress);
                if attempt >= max_retries {
                    return Err(err).context("Download request failed");
                }
                continue;
            }
        };

        if response.status().as_u16() == 416 && downloaded > 0 {
            fs::rename(&temp_path, dest_path).ok();
            return Ok(());
        }

        if !response.status().is_success() {
            retry_delay(attempt, max_retries, backoff_factor, max_backoff, progress);
            if attempt >= max_retries {
                return Err(anyhow::anyhow!("HTTP {}", response.status()));
            }
            continue;
        }

        let content_range = response.headers().get("content-range").and_then(|v| v.to_str().ok());
        let supports_range = content_range.is_some();
        trace!("Response supports_range={} status={}", supports_range, response.status());

        if let Some(range) = content_range {
            if let Some(total) = range.split('/').last().and_then(|v| v.parse::<u64>().ok()) {
                progress.set_length(total);
                progress.set_position(downloaded);
            }
        } else if let Some(total) = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        {
            let total = total.saturating_add(downloaded);
            progress.set_length(total);
            progress.set_position(downloaded);
        }

        if downloaded > 0 && !supports_range {
            debug!("Server ignored range, restarting download from 0");
            downloaded = 0;
            progress.set_position(0);
        }

        let mut file = if downloaded > 0 {
            OpenOptions::new().create(true).append(true).open(&temp_path)?
        } else {
            File::create(&temp_path)?
        };

        let mut buffer = [0u8; 8192];
        let mut had_error = None;
        loop {
            match response.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    file.write_all(&buffer[..read])?;
                    progress.inc(read as u64);
                }
                Err(err) => {
                    had_error = Some(err);
                    break;
                }
            }
        }

        if let Some(err) = had_error {
            retry_delay(attempt, max_retries, backoff_factor, max_backoff, progress);
            downloaded = temp_path.metadata().map(|m| m.len()).unwrap_or(0);
            if attempt >= max_retries {
                return Err(err).context("Download stream failed");
            }
            continue;
        }

        fs::rename(&temp_path, dest_path).ok();
        debug!("Download saved {}", dest_path.display());
        return Ok(());
    }

    Err(anyhow::anyhow!("Download failed after retries"))
}

fn retry_delay(
    attempt: u32,
    max_retries: u32,
    backoff_factor: f32,
    max_backoff: u64,
    progress: &ProgressBar,
) {
    if attempt >= max_retries {
        return;
    }
    let delay = (backoff_factor * 2f32.powi((attempt - 1) as i32)).min(max_backoff as f32);
    progress.set_message(format!("Retrying in {:.1}s", delay));
    debug!(
        "Retry delay {:.1}s (attempt {}/{})",
        delay, attempt, max_retries
    );
    thread::sleep(Duration::from_secs_f32(delay));
}

fn heal_partial_download(
    conn: &rusqlite::Connection,
    attachment_id: i64,
    dest_path: &Path,
    temp_path: &Path,
    timeout_seconds: u64,
) -> Result<()> {
    if let Some(existing) = db::get_download_status(conn, attachment_id)? {
        if existing.status == "processing" {
            debug!("Reset stale processing state id={}", attachment_id);
            db::mark_download_failed(conn, attachment_id)?;
        }
    }

    if temp_path.exists() {
        if dest_path.exists() {
            debug!(
                "Removing stale temp {} (dest exists)",
                temp_path.display()
            );
            fs::remove_file(temp_path).ok();
            return Ok(());
        }

        let size = temp_path.metadata().map(|m| m.len()).unwrap_or(0);
        if size == 0 {
            debug!("Removing empty temp {}", temp_path.display());
            fs::remove_file(temp_path).ok();
            return Ok(());
        }

        let stale_after = if timeout_seconds > 0 {
            timeout_seconds.max(300)
        } else {
            3600
        };
        let is_stale = temp_path
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|m| SystemTime::now().duration_since(m).ok())
            .map(|age| age.as_secs() >= stale_after)
            .unwrap_or(false);

        if is_stale {
            debug!("Removing stale temp {}", temp_path.display());
            fs::remove_file(temp_path).ok();
        }
    }

    Ok(())
}
