use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub fn path_from_slash(path: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        out.push(part);
    }
    out
}

pub fn path_to_slash(path: &Path) -> String {
    let mut parts = Vec::new();
    for comp in path.components() {
        if let std::path::Component::Normal(part) = comp {
            parts.push(part.to_string_lossy().to_string());
        }
    }
    parts.join("/")
}

pub fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            log::trace!("ensure parent {}", parent.display());
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}", parent.display()))?;
        }
    }
    Ok(())
}

pub fn remove_any(path: &Path) -> Result<()> {
    log::trace!("remove {}", path.display());
    if path.is_dir() {
        fs::remove_dir_all(path).ok();
    } else {
        fs::remove_file(path).ok();
    }
    Ok(())
}

pub fn move_with_fallback(src: &Path, dest: &Path) -> Result<()> {
    ensure_parent(dest)?;
    log::trace!("move {} -> {}", src.display(), dest.display());
    if let Err(err) = fs::rename(src, dest) {
        log::debug!("rename failed ({}), falling back to copy", err);
        copy_file(src, dest)?;
        remove_any(src)?;
    }
    Ok(())
}

fn copy_file(src: &Path, dest: &Path) -> Result<()> {
    let mut reader = fs::File::open(src)
        .with_context(|| format!("Failed to open {}", src.display()))?;
    let mut writer = fs::File::create(dest)
        .with_context(|| format!("Failed to create {}", dest.display()))?;
    std::io::copy(&mut reader, &mut writer)
        .with_context(|| format!("Failed to copy to {}", dest.display()))?;
    Ok(())
}
