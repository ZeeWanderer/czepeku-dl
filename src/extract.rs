use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use log::warn;
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
pub struct PartGroup {
    pub base_name: String,
    pub base_path: PathBuf,
    pub part_paths: Vec<PathBuf>,
}

pub fn find_part_groups(root: &Path) -> Result<Vec<PartGroup>> {
    if !root.is_dir() {
        return Ok(Vec::new());
    }

    let mut groups: std::collections::HashMap<String, Vec<PathBuf>> = std::collections::HashMap::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = path.file_name().and_then(|v| v.to_str()).unwrap_or("");
        if let Some((base, _)) = split_part_suffix(name) {
            groups.entry(base).or_default().push(path);
        }
    }

    let mut out = Vec::new();
    for (base, mut parts) in groups {
        if parts.len() < 2 {
            continue;
        }
        parts.sort();
        out.push(PartGroup {
            base_name: base.clone(),
            base_path: root.join(&base),
            part_paths: parts,
        });
    }
    Ok(out)
}

pub fn merge_part_group(group: &PartGroup, dry_run: bool) -> Result<bool> {
    let base_path = &group.base_path;
    if base_path.exists() && !base_path.is_dir() {
        warn!("Skip merge {}: base path is not a directory", base_path.display());
        return Ok(false);
    }

    let mut used: HashSet<PathBuf> = HashSet::new();
    if base_path.exists() {
        for entry in WalkDir::new(base_path).into_iter().filter_map(Result::ok) {
            if entry.path().is_file() {
                if let Ok(rel) = entry.path().strip_prefix(base_path) {
                    used.insert(rel.to_path_buf());
                }
            }
        }
    }

    for part in &group.part_paths {
        for entry in WalkDir::new(part).into_iter().filter_map(Result::ok) {
            if entry.path().is_file() {
                let rel = match entry.path().strip_prefix(part) {
                    Ok(rel) => rel.to_path_buf(),
                    Err(_) => continue,
                };
                if used.contains(&rel) {
                    warn!(
                        "Skip merge {}: conflict on {}",
                        group.base_name,
                        rel.display()
                    );
                    return Ok(false);
                }
                used.insert(rel);
            }
        }
    }

    if dry_run {
        return Ok(true);
    }

    fs::create_dir_all(base_path)
        .with_context(|| format!("Failed to create {}", base_path.display()))?;

    for part in &group.part_paths {
        for entry in WalkDir::new(part).into_iter().filter_map(Result::ok) {
            if entry.path().is_file() {
                let rel = match entry.path().strip_prefix(part) {
                    Ok(rel) => rel,
                    Err(_) => continue,
                };
                let dest = base_path.join(rel);
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent).ok();
                }
                move_file(entry.path(), &dest)?;
            }
        }
        fs::remove_dir_all(part).ok();
    }

    Ok(true)
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
                fs::remove_dir_all(path).ok();
            }
            continue;
        }
        if path.file_name().and_then(|v| v.to_str()) == Some(".DS_Store") {
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
