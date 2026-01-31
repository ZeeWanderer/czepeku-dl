use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;
use zip::ZipArchive;

pub fn extract_zip(file_path: &Path, extract_root: &Path, keep_zip: bool) -> Result<PathBuf> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open zip {}", file_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Invalid zip {}", file_path.display()))?;

    let base_name = file_path
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("archive")
        .trim();

    let mut top_dirs = HashSet::new();
    let mut has_supplements = false;

    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        let name = file.name();
        if is_junk_entry(name) {
            continue;
        }
        if let Some(top) = name.split('/').next() {
            let top = top.trim();
            if top.is_empty() {
                continue;
            }
            if is_supplement_name(top) {
                has_supplements = true;
                continue;
            }
            if !top.starts_with("__MACOSX") {
                top_dirs.insert(top.to_string());
            }
        }
    }

    let target = if top_dirs.len() == 1 && !has_supplements {
        extract_root.to_path_buf()
    } else {
        extract_root.join(base_name)
    };

    fs::create_dir_all(&target)
        .with_context(|| format!("Failed to create extraction dir {}", target.display()))?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if is_junk_entry(&name) {
            continue;
        }
        let relative = sanitize_path(&name);
        if relative.as_os_str().is_empty() {
            continue;
        }
        let out_path = target.join(relative);
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

    clean_junk(&target)?;
    extract_nested_zips(&target, keep_zip)?;

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

fn is_junk_entry(name: &str) -> bool {
    name.starts_with("__MACOSX/")
        || name.ends_with("/.DS_Store")
        || name.ends_with(".DS_Store")
}

fn is_supplement_name(name: &str) -> bool {
    name.starts_with("Gridded") || name.starts_with("Gridless") || name.starts_with("Supplement")
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

fn extract_nested_zips(target: &Path, keep_zip: bool) -> Result<()> {
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
        let extracted = extract_zip(&zip_path, parent, keep_zip)?;
        let _ = extracted;
    }
    Ok(())
}
