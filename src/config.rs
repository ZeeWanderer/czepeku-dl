use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct CreatorConfig {
    pub user_id: String,
    pub posts: HashMap<String, String>,
}

pub type UsersPostsConfig = HashMap<String, CreatorConfig>;

pub fn load_users_posts(path: &Path) -> Result<UsersPostsConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file {}", path.display()))?;
    let config: UsersPostsConfig = serde_json::from_str(&content)
        .with_context(|| format!("Invalid JSON in {}", path.display()))?;
    Ok(config)
}
