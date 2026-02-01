#![allow(dead_code)]
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use log::{debug, trace, warn};
use crate::fsops::path_to_slash;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;
use zip::ZipArchive;

pub fn extract_zip(
    file_path: &Path,
    extract_root: &Path,
    keep_zip: bool,
    max_unpacked_bytes: Option<u64>,
) -> Result<PathBuf> {
    let mut visited = HashSet::new();
    extract_zip_inner(file_path, extract_root, keep_zip, max_unpacked_bytes, &mut visited)
}

#[derive(Debug, Clone)]
pub struct IndexedFile {
    pub rel_path: String,
    pub size: u64,
    pub sha256: String,
}

pub fn extract_zip_to_dir(
    file_path: &Path,
    extract_root: &Path,
    keep_zip: bool,
    max_unpacked_bytes: Option<u64>,
) -> Result<()> {
    debug!(
        "Extract start zip={} dest={} keep_zip={} max_unpacked_bytes={:?}",
        file_path.display(),
        extract_root.display(),
        keep_zip,
        max_unpacked_bytes
    );
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open zip {}", file_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Invalid zip {}", file_path.display()))?;

    let mut total_unpacked: u64 = 0;

    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        if is_symlink(&file) {
            continue;
        }
        let name = file.name();
        if is_junk_entry(name) {
            continue;
        }
        let relative = sanitize_path(name);
        if relative.as_os_str().is_empty() {
            continue;
        }
        if !file.is_dir() {
            total_unpacked = total_unpacked.saturating_add(file.size());
            if let Some(max_bytes) = max_unpacked_bytes {
                if total_unpacked > max_bytes {
                    return Err(anyhow::anyhow!(
                        "Archive exceeds max unpacked size ({} bytes)",
                        max_bytes
                    ));
                }
            }
        }
    }

    fs::create_dir_all(extract_root)
        .with_context(|| format!("Failed to create {}", extract_root.display()))?;

    let mut extracted_files = 0usize;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if is_symlink(&entry) {
            continue;
        }
        let name = entry.name().to_string();
        if is_junk_entry(&name) {
            continue;
        }
        let relative = sanitize_path(&name);
        if relative.as_os_str().is_empty() {
            continue;
        }
        let out_path = extract_root.join(&relative);
        if entry.is_dir() {
            fs::create_dir_all(&out_path).ok();
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).ok();
        }

        if out_path.exists() {
            if out_path.is_dir() {
                fs::remove_dir_all(&out_path).ok();
            } else {
                fs::remove_file(&out_path).ok();
            }
        }

        trace!("Extract file {} -> {}", name, out_path.display());
        let mut outfile = File::create(&out_path)
            .with_context(|| format!("Failed to create file {}", out_path.display()))?;
        std::io::copy(&mut entry, &mut outfile)
            .with_context(|| format!("Failed to extract {}", name))?;
        extracted_files += 1;
    }

    clean_junk(extract_root)?;
    let mut visited = HashSet::new();
    extract_nested_zips(extract_root, keep_zip, max_unpacked_bytes, &mut visited)?;

    if !keep_zip {
        fs::remove_file(file_path).ok();
    }

    debug!(
        "Extract complete zip={} files={} total_unpacked={} bytes",
        file_path.display(),
        extracted_files,
        total_unpacked
    );
    Ok(())
}

pub fn index_extracted_files(root: &Path) -> Result<Vec<IndexedFile>> {
    let mut files = Vec::new();
    if !root.is_dir() {
        return Ok(files);
    }

    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if !entry.path().is_file() {
            continue;
        }
        let rel = match entry.path().strip_prefix(root) {
            Ok(path) => path,
            Err(_) => continue,
        };
        if is_junk_path(rel) {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        let sha256 = hash_file(entry.path())?;
        files.push(IndexedFile {
            rel_path: path_to_slash(rel),
            size,
            sha256,
        });
    }

    Ok(files)
}

#[allow(dead_code)]
pub fn normalize_supplement_folder(root: &Path, dry_run: bool) -> Result<bool> {
    if !root.is_dir() {
        return Ok(false);
    }

    let mut subdirs = Vec::new();
    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            subdirs.push(path);
        } else {
            files.push(path);
        }
    }

    if !files.is_empty() || subdirs.len() != 1 {
        return Ok(false);
    }

    let subdir = &subdirs[0];
    let name = subdir
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("");
    if !is_supplement_name(name) {
        return Ok(false);
    }

    if dry_run {
        return Ok(true);
    }

    for entry in fs::read_dir(subdir)? {
        let entry = entry?;
        let from = entry.path();
        let to = root.join(entry.file_name());
        if to.exists() {
            if to.is_dir() {
                fs::remove_dir_all(&to).ok();
            } else {
                fs::remove_file(&to).ok();
            }
        }
        fs::rename(&from, &to).with_context(|| {
            format!("Failed to move {} to {}", from.display(), to.display())
        })?;
    }

    fs::remove_dir_all(subdir).ok();
    Ok(true)
}

pub fn normalize_repo_flatten(root: &Path, dry_run: bool) -> Result<usize> {
    if !root.is_dir() {
        return Ok(0);
    }

    let mut dirs = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if entry.path().is_dir() {
            dirs.push(entry.path().to_path_buf());
        }
    }

    dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));

    let mut normalized = 0usize;
    for dir in dirs {
        if normalize_single_subdir(&dir, dry_run)? {
            normalized += 1;
        }
    }

    Ok(normalized)
}

#[derive(Debug, Clone)]
pub struct MergeMapping {
    pub part_path: PathBuf,
    pub base_path: PathBuf,
}

pub fn derive_part_base(name: &str) -> Option<String> {
    split_part_suffix(name).map(|(base, _)| base)
}

pub fn merge_part_folders(
    root: &Path,
    dry_run: bool,
    overwrite: bool,
) -> Result<Vec<MergeMapping>> {
    if !root.is_dir() {
        return Ok(Vec::new());
    }

    let mut part_folders: Vec<(PathBuf, String)> = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = path.file_name().and_then(|v| v.to_str()).unwrap_or("");
        if let Some(base) = derive_part_base(name) {
            part_folders.push((path, base));
        }
    }

    let mut mappings = Vec::new();
    let mut remaining: Vec<(PathBuf, String)> = Vec::new();

    for (part_path, base_name) in part_folders {
        let base_path = root.join(&base_name);
        if base_path.is_dir() {
            if merge_single_part(&part_path, &base_path, dry_run, overwrite)? {
                mappings.push(MergeMapping {
                    part_path,
                    base_path,
                });
            }
        } else {
            remaining.push((part_path, base_name));
        }
    }

    let mut grouped: std::collections::HashMap<String, Vec<PathBuf>> = std::collections::HashMap::new();
    for (part_path, base_name) in remaining {
        grouped.entry(base_name).or_default().push(part_path);
    }

    for (base_name, parts) in grouped {
        if parts.len() < 2 {
            continue;
        }
        let base_path = root.join(&base_name);
        if !dry_run {
            fs::create_dir_all(&base_path)
                .with_context(|| format!("Failed to create {}", base_path.display()))?;
        }
        for part_path in parts {
            if merge_single_part(&part_path, &base_path, dry_run, overwrite)? {
                mappings.push(MergeMapping {
                    part_path,
                    base_path: base_path.clone(),
                });
            }
        }
    }

    Ok(mappings)
}

fn split_part_suffix(name: &str) -> Option<(String, u32)> {
    let trimmed = name.trim_end();
    let lower = trimmed.to_lowercase();
    let idx = lower.rfind(" part ")?;
    let (base, suffix) = trimmed.split_at(idx);
    let num_str = suffix.get(6..)?.trim();
    if num_str.is_empty() || !num_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let num = num_str.parse::<u32>().ok()?;
    let base = base.trim_end().trim_end_matches('-').trim_end().to_string();
    if base.is_empty() {
        return None;
    }
    Some((base, num))
}

fn merge_single_part(part_path: &Path, base_path: &Path, dry_run: bool, overwrite: bool) -> Result<bool> {
    if !can_merge_without_conflicts(part_path, base_path)? && !overwrite {
        warn!(
            "Skip merge into {}: conflict detected",
            base_path.display()
        );
        return Ok(false);
    }
    if dry_run {
        return Ok(true);
    }
    fs::create_dir_all(base_path)
        .with_context(|| format!("Failed to create {}", base_path.display()))?;
    merge_folder_contents(part_path, base_path, overwrite)?;
    Ok(true)
}

fn can_merge_without_conflicts(part_path: &Path, base_path: &Path) -> Result<bool> {
    if !base_path.exists() {
        return Ok(true);
    }
    for entry in WalkDir::new(part_path).into_iter().filter_map(Result::ok) {
        if entry.path().is_file() {
            let rel = match entry.path().strip_prefix(part_path) {
                Ok(rel) => rel,
                Err(_) => continue,
            };
            let dest = base_path.join(rel);
            if dest.exists() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn merge_folder_contents(from_dir: &Path, to_dir: &Path, overwrite: bool) -> Result<()> {
    for entry in WalkDir::new(from_dir).into_iter().filter_map(Result::ok) {
        if entry.path().is_file() {
            let rel = match entry.path().strip_prefix(from_dir) {
                Ok(rel) => rel,
                Err(_) => continue,
            };
            let dest = to_dir.join(rel);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent).ok();
            }
            if dest.exists() {
                if overwrite {
                    if dest.is_dir() {
                        fs::remove_dir_all(&dest).ok();
                    } else {
                        fs::remove_file(&dest).ok();
                    }
                } else {
                    continue;
                }
            }
            move_file(entry.path(), &dest)?;
        }
    }
    fs::remove_dir_all(from_dir).ok();
    Ok(())
}

fn move_file(from: &Path, to: &Path) -> Result<()> {
    match fs::rename(from, to) {
        Ok(_) => Ok(()),
        Err(err) => {
            let cross_device = err.raw_os_error() == Some(18);
            if cross_device {
                fs::copy(from, to)
                    .with_context(|| format!("Failed to copy {} to {}", from.display(), to.display()))?;
                fs::remove_file(from).ok();
                Ok(())
            } else {
                Err(err).with_context(|| format!("Failed to move {} to {}", from.display(), to.display()))
            }
        }
    }
}

fn normalize_single_subdir(root: &Path, dry_run: bool) -> Result<bool> {
    if !root.is_dir() {
        return Ok(false);
    }

    let mut subdirs = Vec::new();
    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            subdirs.push(path);
        } else {
            files.push(path);
        }
    }

    if !files.is_empty() || subdirs.len() != 1 {
        return Ok(false);
    }

    let subdir = &subdirs[0];
    if dry_run {
        return Ok(true);
    }

    for entry in fs::read_dir(subdir)? {
        let entry = entry?;
        let from = entry.path();
        let to = root.join(entry.file_name());
        if to.exists() {
            if to.is_dir() {
                fs::remove_dir_all(&to).ok();
            } else {
                fs::remove_file(&to).ok();
            }
        }
        fs::rename(&from, &to).with_context(|| {
            format!("Failed to move {} to {}", from.display(), to.display())
        })?;
    }

    fs::remove_dir_all(subdir).ok();
    Ok(true)
}

fn extract_zip_inner(
    file_path: &Path,
    extract_root: &Path,
    keep_zip: bool,
    max_unpacked_bytes: Option<u64>,
    visited: &mut HashSet<PathBuf>,
) -> Result<PathBuf> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open zip {}", file_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Invalid zip {}", file_path.display()))?;

    let base_name_raw = file_path
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("archive")
        .trim();
    let base_name = sanitize_dir_name(base_name_raw);

    let mut top_dirs = HashSet::new();
    let mut top_dirs_all = HashSet::new();
    let mut has_supplements = false;
    let mut total_unpacked: u64 = 0;

    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        if is_symlink(&file) {
            continue;
        }
        let name = file.name();
        if is_junk_entry(name) {
            continue;
        }
        let relative = sanitize_path(name);
        if relative.as_os_str().is_empty() {
            continue;
        }
        if let Some(top) = relative.components().next() {
            let top_str = top.as_os_str().to_string_lossy();
            let top_str = top_str.trim();
            if top_str.is_empty() {
                continue;
            }
            top_dirs_all.insert(top_str.to_string());
            if is_supplement_name(top_str) {
                has_supplements = true;
            } else {
                top_dirs.insert(top_str.to_string());
            }
        }
        if !file.is_dir() {
            total_unpacked = total_unpacked.saturating_add(file.size());
            if let Some(max_bytes) = max_unpacked_bytes {
                if total_unpacked > max_bytes {
                    return Err(anyhow::anyhow!(
                        "Archive exceeds max unpacked size ({} bytes)",
                        max_bytes
                    ));
                }
            }
        }
    }

    let target = if top_dirs.len() == 1 && !has_supplements {
        extract_root.to_path_buf()
    } else {
        extract_root.join(base_name)
    };

    let strip_prefix = if target != extract_root && top_dirs_all.len() == 1 {
        let only = top_dirs_all.iter().next().cloned().unwrap_or_default();
        if is_supplement_name(&only) {
            Some(only)
        } else {
            None
        }
    } else {
        None
    };

    let use_temp = target != extract_root;
    let working_target = if use_temp {
        create_temp_dir(&target)?
    } else {
        target.clone()
    };

    fs::create_dir_all(&working_target)
        .with_context(|| format!("Failed to create extraction dir {}", working_target.display()))?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if is_symlink(&entry) {
            continue;
        }
        let name = entry.name().to_string();
        if is_junk_entry(&name) {
            continue;
        }
        let mut relative = sanitize_path(&name);
        if let Some(prefix) = strip_prefix.as_deref() {
            let mut comps = relative.components();
            if let Some(first) = comps.next() {
                if first.as_os_str().to_string_lossy() == prefix {
                    relative = comps.as_path().to_path_buf();
                }
            }
        }
        if relative.as_os_str().is_empty() {
            continue;
        }
        let out_path = working_target.join(relative);
        if entry.is_dir() {
            fs::create_dir_all(&out_path).ok();
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).ok();
        }

        if out_path.exists() {
            if out_path.is_dir() {
                fs::remove_dir_all(&out_path).ok();
            } else {
                fs::remove_file(&out_path).ok();
            }
        }

        let mut outfile = File::create(&out_path)
            .with_context(|| format!("Failed to create file {}", out_path.display()))?;
        std::io::copy(&mut entry, &mut outfile)
            .with_context(|| format!("Failed to extract {}", name))?;
    }

    clean_junk(&working_target)?;
    extract_nested_zips(&working_target, keep_zip, max_unpacked_bytes, visited)?;

    if use_temp {
        if target.exists() {
            fs::remove_dir_all(&target).ok();
        }
        fs::rename(&working_target, &target).with_context(|| {
            format!(
                "Failed to move {} to {}",
                working_target.display(),
                target.display()
            )
        })?;
    }

    if !keep_zip {
        fs::remove_file(file_path).ok();
    }

    Ok(target)
}

fn sanitize_path(raw: &str) -> PathBuf {
    let path = Path::new(raw);
    let mut sanitized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => sanitized.push(part),
            Component::CurDir => {}
            Component::ParentDir => {}
            _ => {}
        }
    }
    sanitized
}

fn sanitize_dir_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if matches!(ch, '<' | '>' | ':' | '\"' | '/' | '\\' | '|' | '?' | '*') {
            out.push('-');
        } else if ch.is_control() {
            out.push('-');
        } else {
            out.push(ch);
        }
    }
    let cleaned = out.trim();
    if cleaned.is_empty() {
        "archive".to_string()
    } else {
        cleaned.to_string()
    }
}

fn is_junk_entry(name: &str) -> bool {
    name.starts_with("__MACOSX/")
        || name.ends_with("/.DS_Store")
        || name.ends_with(".DS_Store")
}

fn is_junk_path(path: &Path) -> bool {
    for comp in path.components() {
        if let Component::Normal(part) = comp {
            if part == "__MACOSX" {
                return true;
            }
        }
    }
    if path.file_name().and_then(|v| v.to_str()) == Some(".DS_Store") {
        return true;
    }
    false
}

pub fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", byte);
    }
    Ok(out)
}

fn is_supplement_name(name: &str) -> bool {
    name.starts_with("Gridded") || name.starts_with("Gridless") || name.starts_with("Supplement")
}

fn is_symlink(entry: &zip::read::ZipFile<'_>) -> bool {
    if let Some(mode) = entry.unix_mode() {
        (mode & 0o170000) == 0o120000
    } else {
        false
    }
}

fn clean_junk(target: &Path) -> Result<()> {
    for entry in WalkDir::new(target).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|v| v.to_str()) == Some("__MACOSX") {
                trace!("Remove junk dir {}", path.display());
                fs::remove_dir_all(path).ok();
            }
            continue;
        }
        if path.file_name().and_then(|v| v.to_str()) == Some(".DS_Store") {
            trace!("Remove junk file {}", path.display());
            fs::remove_file(path).ok();
        }
    }
    Ok(())
}

fn extract_nested_zips(
    target: &Path,
    keep_zip: bool,
    max_unpacked_bytes: Option<u64>,
    visited: &mut HashSet<PathBuf>,
) -> Result<()> {
    let mut nested = Vec::new();
    for entry in WalkDir::new(target).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension().and_then(|v| v.to_str()) {
                if ext.eq_ignore_ascii_case("zip") {
                    nested.push(path.to_path_buf());
                }
            }
        }
    }

    for zip_path in nested {
        let parent = zip_path.parent().unwrap_or(target);
        let key = zip_path.canonicalize().unwrap_or_else(|_| zip_path.clone());
        if !visited.insert(key) {
            continue;
        }
        debug!("Extract nested zip {}", zip_path.display());
        let extracted = extract_zip_inner(&zip_path, parent, keep_zip, max_unpacked_bytes, visited)?;
        let _ = extracted;
    }
    Ok(())
}


fn create_temp_dir(target: &Path) -> Result<PathBuf> {
    let parent = target.parent().unwrap_or(target);
    let name = target
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("extract");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    for attempt in 0..10u32 {
        let candidate = parent.join(format!(
            ".tmp-{}-{}-{}",
            name,
            std::process::id(),
            now + attempt as u128
        ));
        if !candidate.exists() {
            fs::create_dir_all(&candidate).with_context(|| {
                format!("Failed to create temp dir {}", candidate.display())
            })?;
            return Ok(candidate);
        }
    }
    Err(anyhow::anyhow!("Failed to create temp extraction directory"))
}
