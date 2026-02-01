use crate::db::{self, ExtractedFileRow, FoldMapRow};
use crate::fsops::{move_with_fallback, path_from_slash, remove_any};
use anyhow::Result;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Component, Path};
use log::{debug, trace, warn};

const FOLD_ALGO_VERSION: &str = "tree-v1";

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FoldPlan {
    pub repo_root: String,
    pub root_rule: String,
    pub flatten_prefix: Option<String>,
    pub mappings: Vec<FoldMapRow>,
}

pub fn plan_fold(
    conn: &rusqlite::Connection,
    repo_dir: &Path,
    attachment_id: i64,
    attachment_name: &str,
    files: &[ExtractedFileRow],
) -> Result<FoldPlan> {
    if let Some(existing) = db::get_meta(conn, "fold_algo")? {
        if existing != FOLD_ALGO_VERSION {
            warn!(
                "Fold algorithm changed (db={}, current={})",
                existing, FOLD_ALGO_VERSION
            );
            db::set_meta(conn, "fold_algo", FOLD_ALGO_VERSION)?;
        }
    } else {
        db::set_meta(conn, "fold_algo", FOLD_ALGO_VERSION)?;
    }

    let flatten_prefix = compute_flatten_prefix(files);
    let attachment_base = base_name_from_attachment(attachment_name);
    let attachment_no_part = strip_part_suffix(&attachment_base);
    let (root_base, variant_prefix) = strip_variant(&attachment_no_part);
    let base_name = sanitize_dir_name(&attachment_no_part);
    let repo_root_base = sanitize_dir_name(&root_base);

    let existing_root = db::get_attachment_root(conn, attachment_id)?;
    let (repo_root, root_rule) = if let Some(existing) = existing_root {
        if variant_prefix.is_some() && should_override_variant_root(&existing.repo_root, &root_base) {
            (repo_root_base.clone(), "variant".to_string())
        } else {
            (existing.repo_root, existing.rule)
        }
    } else if variant_prefix.is_some() {
        (repo_root_base.clone(), "variant".to_string())
    } else if let Some((base, _part)) = split_part_suffix(&base_name) {
        let group_key = merge_group_key(&root_base, variant_prefix.as_deref(), &base);
        if let Some(merged) = db::get_merge_group(conn, &group_key)? {
            if merged {
                (base, "merged".to_string())
            } else {
                (ensure_unique_root(conn, repo_dir, &base_name, attachment_id)?, "part".to_string())
            }
        } else {
            let candidate_root = base.clone();
            let part_bases = compute_part_bases(
                files,
                flatten_prefix.as_deref(),
                &attachment_base,
            );
            let part_rewrites = build_part_rewrites(
                &part_bases,
                &root_base,
                variant_prefix.as_deref(),
                true,
            );
            let (candidate_paths, paths_error) = match build_repo_paths(
                &candidate_root,
                flatten_prefix.as_deref(),
                &part_rewrites,
                variant_prefix.as_deref(),
                files,
            ) {
                Ok(paths) => (paths, false),
                Err(err) => {
                    warn!(
                        "Fold candidate path build failed for {}: {}",
                        attachment_name, err
                    );
                    (Vec::new(), true)
                }
            };
            if paths_error || has_conflicts(conn, repo_dir, attachment_id, &candidate_paths)? {
                db::set_merge_group(conn, &group_key, false)?;
                (ensure_unique_root(conn, repo_dir, &base_name, attachment_id)?, "part".to_string())
            } else {
                db::set_merge_group(conn, &group_key, true)?;
                (candidate_root, "merged".to_string())
            }
        }
    } else {
        (ensure_unique_root(conn, repo_dir, &base_name, attachment_id)?, "direct".to_string())
    };

    debug!(
        "Fold root id={} name={} rule={} root={}",
        attachment_id, attachment_name, root_rule, repo_root
    );
    db::set_attachment_root(conn, attachment_id, &repo_root, &root_rule)?;

    let part_bases = compute_part_bases(files, flatten_prefix.as_deref(), &attachment_base);
    let mut part_rewrites = build_part_rewrites(
        &part_bases,
        &root_base,
        variant_prefix.as_deref(),
        true,
    );
    let mut mappings = build_mappings(
        conn,
        attachment_id,
        &repo_root,
        &root_rule,
        flatten_prefix.as_deref(),
        &part_rewrites,
        variant_prefix.as_deref(),
        files,
    );

    if mappings.is_err() && part_rewrites.values().any(|v| v.is_none()) {
        warn!(
            "Fold conflicts detected for {}. Falling back to keeping part base folders.",
            attachment_name
        );
        part_rewrites = build_part_rewrites(
            &part_bases,
            &root_base,
            variant_prefix.as_deref(),
            false,
        );
        mappings = build_mappings(
            conn,
            attachment_id,
            &repo_root,
            &root_rule,
            flatten_prefix.as_deref(),
            &part_rewrites,
            variant_prefix.as_deref(),
            files,
        );
    }

    if mappings.is_err() && !part_rewrites.is_empty() {
        warn!(
            "Fold conflicts persist for {}. Falling back to original part folders.",
            attachment_name
        );
        part_rewrites.clear();
        mappings = build_mappings(
            conn,
            attachment_id,
            &repo_root,
            &root_rule,
            flatten_prefix.as_deref(),
            &part_rewrites,
            variant_prefix.as_deref(),
            files,
        );
    }

    let mappings = mappings?;

    Ok(FoldPlan {
        repo_root,
        root_rule,
        flatten_prefix,
        mappings,
    })
}

pub fn apply_plan(
    staging_root: &Path,
    repo_dir: &Path,
    plan: &FoldPlan,
    force: bool,
) -> Result<()> {
    for mapping in &plan.mappings {
        trace!(
            "Apply map {} -> {}",
            mapping.original_path,
            mapping.repo_path
        );
        let src = staging_root.join(path_from_slash(&mapping.original_path));
        let dest = repo_dir.join(path_from_slash(&mapping.repo_path));
        if !src.exists() {
            return Err(anyhow::anyhow!("Missing extracted file {}", src.display()));
        }
        if dest.exists() {
            if force {
                remove_any(&dest)?;
            } else {
                return Err(anyhow::anyhow!("Destination already exists: {}", dest.display()));
            }
        }
        move_with_fallback(&src, &dest)?;
    }
    Ok(())
}

pub fn remove_repo_files(repo_dir: &Path, repo_paths: &[String]) -> Result<usize> {
    let mut removed = 0usize;
    for rel in repo_paths {
        let path = repo_dir.join(path_from_slash(rel));
        if path.exists() {
            trace!("Remove repo path {}", path.display());
            remove_any(&path)?;
            removed += 1;
        }
    }
    Ok(removed)
}

pub fn prune_empty_dirs(repo_dir: &Path, repo_paths: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for rel in repo_paths {
        let mut current = repo_dir.join(path_from_slash(rel));
        while let Some(parent) = current.parent() {
            if parent == repo_dir {
                break;
            }
            let parent_str = parent.to_string_lossy().to_string();
            if !seen.insert(parent_str) {
                current = parent.to_path_buf();
                continue;
            }
            if is_dir_empty(parent)? {
                trace!("Prune empty dir {}", parent.display());
                fs::remove_dir(parent).ok();
            }
            current = parent.to_path_buf();
        }
    }
    Ok(())
}

fn is_dir_empty(path: &Path) -> Result<bool> {
    if !path.is_dir() {
        return Ok(true);
    }
    let mut entries = fs::read_dir(path)?;
    Ok(entries.next().is_none())
}

fn compute_flatten_prefix(files: &[ExtractedFileRow]) -> Option<String> {
    let mut top_dirs = HashSet::new();
    let mut has_root_files = false;

    for file in files {
        let path = Path::new(&file.original_path);
        let mut comps = path.components().filter_map(|c| match c {
            Component::Normal(v) => Some(v.to_string_lossy().to_string()),
            _ => None,
        });
        let first = match comps.next() {
            Some(v) => v,
            None => continue,
        };
        if comps.next().is_none() {
            has_root_files = true;
        } else {
            top_dirs.insert(first);
        }
    }

    if !has_root_files && top_dirs.len() == 1 {
        top_dirs.into_iter().next()
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct TreeNode {
    name: String,
    kind: NodeKind,
    children: BTreeMap<String, TreeNode>,
    original_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeKind {
    Dir,
    File,
}

impl TreeNode {
    fn new_dir(name: String) -> Self {
        Self {
            name,
            kind: NodeKind::Dir,
            children: BTreeMap::new(),
            original_path: None,
        }
    }

    fn new_file(name: String, original_path: String) -> Self {
        Self {
            name,
            kind: NodeKind::File,
            children: BTreeMap::new(),
            original_path: Some(original_path),
        }
    }
}

#[derive(Debug)]
struct TreeEntry {
    original_path: String,
    rel_path: String,
}

fn build_tree_entries(
    files: &[ExtractedFileRow],
    flatten_prefix: Option<&str>,
    part_rewrites: &HashMap<String, Option<String>>,
    variant_prefix: Option<&str>,
) -> Result<Vec<TreeEntry>> {
    let mut root = TreeNode::new_dir(String::new());
    for file in files {
        insert_file(&mut root, &file.original_path);
    }

    apply_flatten_prefix(&mut root, flatten_prefix);
    apply_part_rewrites(&mut root, part_rewrites)?;
    apply_variant_prefix(&mut root, variant_prefix)?;

    let mut entries = Vec::new();
    collect_entries(&root, "", &mut entries);
    Ok(entries)
}

fn insert_file(root: &mut TreeNode, path: &str) {
    let comps: Vec<String> = Path::new(path)
        .components()
        .filter_map(|c| match c {
            Component::Normal(v) => Some(v.to_string_lossy().to_string()),
            _ => None,
        })
        .collect();
    if comps.is_empty() {
        return;
    }

    let mut current = root;
    for (i, name) in comps.iter().enumerate() {
        let is_last = i == comps.len() - 1;
        if is_last {
            let node = TreeNode::new_file(name.clone(), path.to_string());
            current.children.entry(name.clone()).or_insert(node);
        } else {
            let entry = current
                .children
                .entry(name.clone())
                .or_insert_with(|| TreeNode::new_dir(name.clone()));
            if entry.kind != NodeKind::Dir {
                return;
            }
            current = entry;
        }
    }
}

fn apply_flatten_prefix(root: &mut TreeNode, prefix: Option<&str>) {
    let Some(prefix) = prefix else { return; };
    if let Some(node) = root.children.remove(prefix) {
        if node.kind == NodeKind::Dir && root.children.is_empty() {
            root.children = node.children;
        } else {
            root.children.insert(prefix.to_string(), node);
        }
    }
}

fn apply_part_rewrites(
    root: &mut TreeNode,
    part_rewrites: &HashMap<String, Option<String>>,
) -> Result<()> {
    if part_rewrites.is_empty() {
        return Ok(());
    }
    let keys: Vec<String> = part_rewrites.keys().cloned().collect();
    for key in keys {
        let rewrite = part_rewrites.get(&key).cloned().unwrap_or(None);
        let Some(node) = root.children.remove(&key) else { continue; };
        if node.kind != NodeKind::Dir {
            root.children.insert(key, node);
            continue;
        }
        match rewrite {
            None => merge_node_into_dir(root, node)?,
            Some(dest_name) => {
                if dest_name == key {
                    root.children.insert(key, node);
                    continue;
                }
                let dest = root
                    .children
                    .entry(dest_name.clone())
                    .or_insert_with(|| TreeNode::new_dir(dest_name.clone()));
                if dest.kind != NodeKind::Dir {
                    return Err(anyhow::anyhow!(
                        "Part rewrite conflict: {} is not a directory",
                        dest_name
                    ));
                }
                merge_node_into_dir(dest, node)?;
            }
        }
    }
    Ok(())
}

fn apply_variant_prefix(root: &mut TreeNode, variant_prefix: Option<&str>) -> Result<()> {
    let Some(variant) = variant_prefix else { return Ok(()); };
    if root.children.len() == 1 {
        if let Some(node) = root.children.get(variant) {
            if node.kind == NodeKind::Dir {
                return Ok(());
            }
        }
    }

    let mut dest = if let Some(node) = root.children.remove(variant) {
        if node.kind != NodeKind::Dir {
            return Err(anyhow::anyhow!(
                "Variant path conflict: {} is not a directory",
                variant
            ));
        }
        node
    } else {
        TreeNode::new_dir(variant.to_string())
    };

    let keys: Vec<String> = root.children.keys().cloned().collect();
    for key in keys {
        if key == variant {
            continue;
        }
        if let Some(node) = root.children.remove(&key) {
            merge_node_into_dir(&mut dest, node)?;
        }
    }

    root.children.insert(variant.to_string(), dest);
    Ok(())
}

fn merge_node_into_dir(dest: &mut TreeNode, mut node: TreeNode) -> Result<()> {
    if node.kind != NodeKind::Dir {
        let name = node.name.clone();
        if let Some(existing) = dest.children.get(&name) {
            return Err(anyhow::anyhow!(
                "Path collision while merging {}",
                existing.name
            ));
        }
        dest.children.insert(name, node);
        return Ok(());
    }

    let children = std::mem::take(&mut node.children);
    for (_name, child) in children {
        let name = child.name.clone();
        if let Some(existing) = dest.children.get_mut(&name) {
            if existing.kind == NodeKind::Dir && child.kind == NodeKind::Dir {
                merge_node_into_dir(existing, child)?;
            } else {
                return Err(anyhow::anyhow!(
                    "Path collision while merging {}",
                    name
                ));
            }
        } else {
            dest.children.insert(name, child);
        }
    }
    Ok(())
}

fn collect_entries(node: &TreeNode, prefix: &str, out: &mut Vec<TreeEntry>) {
    match node.kind {
        NodeKind::File => {
            let rel = if prefix.is_empty() {
                node.name.clone()
            } else if prefix == node.name || prefix.ends_with(&format!("/{}", node.name)) {
                prefix.to_string()
            } else {
                format!("{}/{}", prefix, node.name)
            };
            if let Some(original) = node.original_path.clone() {
                out.push(TreeEntry {
                    original_path: original,
                    rel_path: rel,
                });
            }
        }
        NodeKind::Dir => {
            for child in node.children.values() {
                let mut next = prefix.to_string();
                if !child.name.is_empty() {
                    if !next.is_empty() {
                        next.push('/');
                    }
                    next.push_str(&child.name);
                }
                collect_entries(child, &next, out);
            }
        }
    }
}

fn build_mappings(
    conn: &rusqlite::Connection,
    attachment_id: i64,
    repo_root: &str,
    rule: &str,
    flatten_prefix: Option<&str>,
    part_rewrites: &HashMap<String, Option<String>>,
    variant_prefix: Option<&str>,
    files: &[ExtractedFileRow],
) -> Result<Vec<FoldMapRow>> {
    let entries = build_tree_entries(files, flatten_prefix, part_rewrites, variant_prefix)?;
    let mut seen_repo = HashSet::new();
    let mut mappings = Vec::new();
    for entry in entries {
        let repo_path = join_repo_path(repo_root, &entry.rel_path);
        if !seen_repo.insert(repo_path.clone()) {
            return Err(anyhow::anyhow!("Duplicate repo path detected: {}", repo_path));
        }
        if db::repo_path_in_use(conn, &repo_path, attachment_id)? {
            return Err(anyhow::anyhow!("Repo path already in use: {}", repo_path));
        }
        mappings.push(FoldMapRow {
            original_path: entry.original_path,
            repo_path,
            rule: rule.to_string(),
        });
    }
    Ok(mappings)
}

fn normalize_path(
    path: &str,
    flatten_prefix: Option<&str>,
    part_rewrites: &HashMap<String, Option<String>>,
    variant_prefix: Option<&str>,
) -> String {
    let mut comps: Vec<String> = Path::new(path)
        .components()
        .filter_map(|c| match c {
            Component::Normal(v) => Some(v.to_string_lossy().to_string()),
            _ => None,
        })
        .collect();

    if comps.is_empty() {
        return String::new();
    }

    if let Some(prefix) = flatten_prefix {
        if comps.first().map(|v| v == prefix).unwrap_or(false) {
            comps.remove(0);
        }
    }

    if comps.is_empty() {
        return String::new();
    }

    if let Some(rewrite) = part_rewrites.get(&comps[0]) {
        match rewrite {
            Some(to) => comps[0] = to.clone(),
            None => {
                comps.remove(0);
            }
        }
    }

    if let Some(variant) = variant_prefix {
        let needs_variant = comps
            .first()
            .map(|v| !v.eq_ignore_ascii_case(variant))
            .unwrap_or(true);
        if needs_variant {
            comps.insert(0, variant.to_string());
        }
    }

    comps.join("/")
}

fn join_repo_path(root: &str, rel: &str) -> String {
    if rel.is_empty() {
        root.to_string()
    } else {
        format!("{}/{}", root.trim_end_matches('/'), rel)
    }
}

fn compute_part_bases(
    files: &[ExtractedFileRow],
    flatten_prefix: Option<&str>,
    attachment_base: &str,
) -> HashMap<String, String> {
    let mut groups: HashMap<String, HashSet<String>> = HashMap::new();
    let rewrites: HashMap<String, Option<String>> = HashMap::new();
    for file in files {
        let normalized = normalize_path(&file.original_path, flatten_prefix, &rewrites, None);
        let first = normalized.split('/').next().filter(|v| !v.is_empty()).map(|v| v.to_string());
        let Some(first) = first else { continue; };
        if let Some((base, _)) = split_part_suffix(&first) {
            groups.entry(base).or_default().insert(first);
        }
    }

    let mut out = HashMap::new();
    let attachment_is_part = split_part_suffix(attachment_base).is_some();

    for (base, parts) in groups {
        if parts.len() < 2 && !attachment_is_part {
            continue;
        }
        for part in parts {
            out.insert(part, base.clone());
        }
    }
    out
}

fn build_part_rewrites(
    part_bases: &HashMap<String, String>,
    root_base: &str,
    variant_prefix: Option<&str>,
    allow_drop: bool,
) -> HashMap<String, Option<String>> {
    let root_lower = root_base.to_lowercase();
    let mut out = HashMap::new();
    for (part, base) in part_bases {
        let base_lower = base.to_lowercase();
        let drop = allow_drop && base_lower == root_lower;
        if drop {
            out.insert(part.clone(), None);
        } else if let Some(variant) = variant_prefix {
            if base_lower == variant.to_lowercase() {
                out.insert(part.clone(), Some(variant.to_string()));
            } else {
                out.insert(part.clone(), Some(base.clone()));
            }
        } else {
            out.insert(part.clone(), Some(base.clone()));
        }
    }
    out
}

fn has_conflicts(
    conn: &rusqlite::Connection,
    repo_dir: &Path,
    attachment_id: i64,
    repo_paths: &[String],
) -> Result<bool> {
    for rel in repo_paths {
        if db::repo_path_in_use(conn, rel, attachment_id)? {
            return Ok(true);
        }
        let path = repo_dir.join(path_from_slash(rel));
        if path.exists() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn build_repo_paths(
    root: &str,
    prefix: Option<&str>,
    part_rewrites: &HashMap<String, Option<String>>,
    variant_prefix: Option<&str>,
    files: &[ExtractedFileRow],
) -> Result<Vec<String>> {
    let entries = build_tree_entries(files, prefix, part_rewrites, variant_prefix)?;
    Ok(entries
        .into_iter()
        .map(|entry| join_repo_path(root, &entry.rel_path))
        .collect())
}

fn ensure_unique_root(
    conn: &rusqlite::Connection,
    repo_dir: &Path,
    base: &str,
    attachment_id: i64,
) -> Result<String> {
    let mut candidate = base.to_string();
    let mut counter = 2u32;
    loop {
        let in_use = db::repo_root_in_use(conn, &candidate, attachment_id)?;
        let exists = repo_dir.join(path_from_slash(&candidate)).exists();
        if !in_use && !exists {
            return Ok(candidate);
        }
        candidate = format!("{} ({})", base, counter);
        counter += 1;
    }
}

fn base_name_from_attachment(name: &str) -> String {
    let stem = Path::new(name)
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or(name);
    stem.trim().to_string()
}

fn strip_part_suffix(name: &str) -> String {
    if let Some((base, _)) = split_part_suffix(name) {
        base
    } else {
        name.to_string()
    }
}

fn strip_variant(name: &str) -> (String, Option<String>) {
    let lower = name.to_lowercase();
    let variant = if lower.contains("gridless") {
        Some("Gridless".to_string())
    } else if lower.contains("gridded") {
        Some("Gridded".to_string())
    } else {
        None
    };

    let Some(variant_label) = variant.clone() else {
        return (name.to_string(), None);
    };

    let mut tokens = Vec::new();
    for token in name.split_whitespace() {
        let cleaned: String = token
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .collect::<String>()
            .to_lowercase();
        if cleaned == variant_label.to_lowercase() {
            continue;
        }
        tokens.push(token);
    }

    let mut cleaned = tokens.join(" ");
    cleaned = cleaned.replace("()", "");
    cleaned = cleaned.replace("[]", "");
    while cleaned.contains("  ") {
        cleaned = cleaned.replace("  ", " ");
    }
    let cleaned = cleaned
        .trim_matches(|c: char| c.is_whitespace() || c == '-' || c == '–' || c == '—')
        .to_string();

    (cleaned, Some(variant_label))
}

fn merge_group_key(root_base: &str, variant: Option<&str>, part_base: &str) -> String {
    let root = root_base.trim();
    let part = part_base.trim();
    if let Some(variant) = variant {
        if part.eq_ignore_ascii_case(variant) {
            return format!("{}::{}", root, variant);
        }
        return format!("{}::{}::{}", root, variant, part);
    }
    if root.eq_ignore_ascii_case(part) {
        root.to_string()
    } else {
        format!("{}::{}", root, part)
    }
}

fn should_override_variant_root(existing_root: &str, desired_root: &str) -> bool {
    let (_, existing_variant) = strip_variant(existing_root);
    existing_variant.is_some() && !existing_root.eq_ignore_ascii_case(desired_root)
}

fn sanitize_dir_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn build_files(paths: &[&str]) -> Vec<ExtractedFileRow> {
        paths
            .iter()
            .map(|path| ExtractedFileRow {
                original_path: (*path).to_string(),
                size: None,
                sha256: None,
            })
            .collect()
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let base = std::env::temp_dir();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            for attempt in 0..20u32 {
                let candidate = base.join(format!(
                    "czepeku-test-{}-{}-{}",
                    prefix,
                    std::process::id(),
                    now + attempt as u128
                ));
                if candidate.exists() {
                    continue;
                }
                fs::create_dir_all(&candidate).expect("create tempdir");
                return Self { path: candidate };
            }
            panic!("Failed to create temp dir");
        }

        fn path(&self) -> &PathBuf {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn setup() -> (TempDir, rusqlite::Connection, PathBuf) {
        let dir = TempDir::new("fold");
        let db_path = dir.path().join("index.sqlite");
        let conn = db::open_db(&db_path).expect("open db");
        let repo_dir = dir.path().join("repo");
        fs::create_dir_all(&repo_dir).expect("create repo");
        (dir, conn, repo_dir)
    }

    #[test]
    fn plan_fold_merges_parts_and_variants() {
        let (_dir, conn, repo_dir) = setup();
        let files = build_files(&["Gridded Part 1/a.png", "Gridded Part 2/b.png"]);
        let plan = plan_fold(
            &conn,
            &repo_dir,
            1,
            "Yggdrasil Roots Gridded Part 1.zip",
            &files,
        )
        .expect("plan");

        assert_eq!(plan.repo_root, "Yggdrasil Roots");
        assert!(plan
            .mappings
            .iter()
            .all(|m| m.repo_path.starts_with("Yggdrasil Roots/Gridded/")));
        assert!(plan.mappings.iter().all(|m| !m.repo_path.contains("Part ")));
    }

    #[test]
    fn plan_fold_variant_roots_shared() {
        let (_dir, conn, repo_dir) = setup();
        let gridded = build_files(&["Gridded Part 1/a.png", "Gridded Part 2/b.png"]);
        let gridless = build_files(&["Gridless Part 1/c.png", "Gridless Part 2/d.png"]);

        let plan_a = plan_fold(
            &conn,
            &repo_dir,
            1,
            "Temple Gridless Part 1.zip",
            &gridless,
        )
        .expect("plan a");
        let plan_b = plan_fold(
            &conn,
            &repo_dir,
            2,
            "Temple Gridded Part 1.zip",
            &gridded,
        )
        .expect("plan b");

        assert_eq!(plan_a.repo_root, "Temple");
        assert_eq!(plan_b.repo_root, "Temple");
        assert!(plan_a
            .mappings
            .iter()
            .all(|m| m.repo_path.starts_with("Temple/Gridless/")));
        assert!(plan_b
            .mappings
            .iter()
            .all(|m| m.repo_path.starts_with("Temple/Gridded/")));
    }

    #[test]
    fn plan_fold_flattens_single_root_folder() {
        let (_dir, conn, repo_dir) = setup();
        let files = build_files(&["Map/file1.png", "Map/sub/file2.png"]);
        let plan = plan_fold(&conn, &repo_dir, 3, "Map.zip", &files).expect("plan");

        assert_eq!(plan.repo_root, "Map");
        assert!(plan
            .mappings
            .iter()
            .all(|m| !m.repo_path.starts_with("Map/Map/")));
    }

    #[test]
    fn strip_variant_removes_token() {
        let (base, variant) = strip_variant("My Map - Gridded");
        assert_eq!(base, "My Map");
        assert_eq!(variant.as_deref(), Some("Gridded"));
    }

    #[test]
    fn strip_part_suffix_removes_part() {
        let base = strip_part_suffix("Dungeon Part 2");
        assert_eq!(base, "Dungeon");
    }

    #[test]
    fn tree_adds_variant_prefix_when_missing() {
        let files = build_files(&["a.png"]);
        let entries = build_tree_entries(&files, None, &HashMap::new(), Some("Gridded"))
            .expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].rel_path, "Gridded/a.png");
    }

    #[test]
    fn tree_keeps_existing_variant_folder() {
        let files = build_files(&["Gridded/a.png"]);
        let entries = build_tree_entries(&files, None, &HashMap::new(), Some("Gridded"))
            .expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].rel_path, "Gridded/a.png");
    }

    #[test]
    fn tree_drops_part_folder_when_rewrite_none() {
        let files = build_files(&["Part 1/a.png"]);
        let mut rewrites = HashMap::new();
        rewrites.insert("Part 1".to_string(), None);
        let entries = build_tree_entries(&files, None, &rewrites, None).expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].rel_path, "a.png");
    }

    #[test]
    fn tree_merges_part_into_existing_folder() {
        let files = build_files(&["Part 1/a.png", "Gridded/b.png"]);
        let mut rewrites = HashMap::new();
        rewrites.insert("Part 1".to_string(), Some("Gridded".to_string()));
        let mut entries = build_tree_entries(&files, None, &rewrites, None).expect("entries");
        entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].rel_path, "Gridded/a.png");
        assert_eq!(entries[1].rel_path, "Gridded/b.png");
    }

    #[test]
    fn tree_detects_merge_collision() {
        let files = build_files(&["Part 1/a.png", "Gridded/a.png"]);
        let mut rewrites = HashMap::new();
        rewrites.insert("Part 1".to_string(), Some("Gridded".to_string()));
        let result = build_tree_entries(&files, None, &rewrites, None);
        assert!(result.is_err());
    }

    #[test]
    fn plan_fold_uses_unique_root_when_existing() {
        let (_dir, conn, repo_dir) = setup();
        let files = build_files(&["Map/a.png"]);
        let existing = repo_dir.join("Map");
        fs::create_dir_all(&existing).expect("create existing");
        fs::write(existing.join("a.png"), b"hi").expect("write existing");

        let plan = plan_fold(&conn, &repo_dir, 9, "Map.zip", &files).expect("plan");
        assert_ne!(plan.repo_root, "Map");
    }
}
