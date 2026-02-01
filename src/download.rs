use crate::db::{self, AttachmentRow};
use crate::extract;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::blocking::Client;
use reqwest::header::RANGE;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub base_url: String,
    pub download_dir: PathBuf,
    pub repo_dir: PathBuf,
    pub keep_zip: bool,
    pub max_unpacked_bytes: Option<u64>,
    pub max_retries: u32,
    pub backoff_factor: f32,
    pub max_backoff: u64,
    pub workers: usize,
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

    fs::create_dir_all(&options.download_dir)
        .with_context(|| format!("Failed to create {}", options.download_dir.display()))?;
    fs::create_dir_all(&options.repo_dir)
        .with_context(|| format!("Failed to create {}", options.repo_dir.display()))?;

    let queue = Arc::new(Mutex::new(items));
    let mut handles = Vec::new();
    let workers = options.workers.max(1);

    for worker_id in 0..workers {
        let queue = Arc::clone(&queue);
        let client = client.clone();
        let options = options.clone();
        let db_path = db_path.to_path_buf();

        handles.push(thread::spawn(move || {
            loop {
                let item = {
                    let mut guard = queue.lock().unwrap();
                    guard.pop()
                };

                let Some(item) = item else { break; };
                if let Err(err) = process_item(&db_path, &client, &options, &item, worker_id) {
                    eprintln!("Failed: {} ({})", item.name, err);
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}

fn process_item(
    db_path: &Path,
    client: &Client,
    options: &DownloadOptions,
    item: &AttachmentRow,
    worker_id: usize,
) -> Result<()> {
    let conn = db::open_db(db_path)?;

    if let Some(existing) = db::get_download_status(&conn, &item.path)? {
        if existing.status == "completed" {
            println!("Skip (done): {}", item.name);
            return Ok(());
        }
    }

    if let Some(ref sha) = item.sha256 {
        if let Some(existing) = db::find_completed_by_sha256(&conn, sha)? {
            let extract_ok = existing
                .extract_path
                .as_ref()
                .map(|p| options.repo_dir.join(p).exists())
                .unwrap_or(false);
            if extract_ok {
                db::mark_download_completed(
                    &conn,
                    &item.path,
                    item.id,
                    item.sha256.as_deref(),
                    item.size,
                    existing.local_zip.as_deref(),
                    existing.extract_path.as_deref(),
                )?;
                println!("Skip (sha256 match): {}", item.name);
                return Ok(());
            }
        }
    }

    db::mark_download_processing(
        &conn,
        &item.path,
        item.id,
        item.sha256.as_deref(),
        item.size,
    )?;

    let url = build_download_url(options, item);
    let zip_path = choose_zip_path(&options.download_dir, &item.name, &item.path);

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("{spinner} {msg}").unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    progress.set_message(format!("[{}] {}", worker_id + 1, item.name));

    let download_result = download_with_resume(
        client,
        &url,
        &zip_path,
        options.max_retries,
        options.backoff_factor,
        options.max_backoff,
        &progress,
    );

    match download_result {
        Ok(()) => {
            progress.finish_with_message(format!("Downloaded {}", item.name));
            let extract_path = extract::extract_zip(
                &zip_path,
                &options.repo_dir,
                options.keep_zip,
                options.max_unpacked_bytes,
            )?;
            let rel_extract = extract_path
                .strip_prefix(&options.repo_dir)
                .unwrap_or(&extract_path)
                .to_string_lossy()
                .to_string();
            db::mark_download_completed(
                &conn,
                &item.path,
                item.id,
                item.sha256.as_deref(),
                item.size,
                Some(&zip_path.to_string_lossy()),
                Some(&rel_extract),
            )?;
            println!("Done: {}", item.name);
            Ok(())
        }
        Err(err) => {
            progress.finish_with_message(format!("Failed {}", item.name));
            db::mark_download_failed(&conn, &item.path)?;
            Err(err)
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
    progress: &ProgressBar,
) -> Result<()> {
    let temp_path = dest_path.with_extension("part");
    let mut downloaded = temp_path.metadata().map(|m| m.len()).unwrap_or(0);

    let mut attempt = 0;
    while attempt < max_retries {
        attempt += 1;
        progress.set_message(format!("Downloading (attempt {})", attempt));

        let mut request = client.get(url);
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

        let supports_range = response
            .headers()
            .get("content-range")
            .is_some();

        if downloaded > 0 && !supports_range {
            downloaded = 0;
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
    thread::sleep(Duration::from_secs_f32(delay));
}
