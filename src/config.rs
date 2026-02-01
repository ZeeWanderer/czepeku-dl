use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CreatorConfig {
    pub user_id: String,
    pub posts: HashMap<String, String>,
}

pub type UsersPostsConfig = HashMap<String, CreatorConfig>;

#[derive(Clone, Copy)]
struct CreatorSeed {
    name: &'static str,
    user_id: &'static str,
    posts: &'static [(&'static str, &'static str)],
}

const DEFAULT_CREATORS: &[CreatorSeed] = &[
    CreatorSeed {
        name: "czepeku",
        user_id: "16010661",
        posts: &[("main", "27816327"), ("animated", "49184370")],
    },
    CreatorSeed {
        name: "czepekuscifi",
        user_id: "74462793",
        posts: &[("main", "67639434"), ("animated", "70330505")],
    },
    CreatorSeed {
        name: "czepekuscenes",
        user_id: "85204685",
        posts: &[("main", "79200795"), ("animated", "79200864")],
    },
];

pub fn default_creators_json() -> String {
    let mut out: UsersPostsConfig = HashMap::new();
    for seed in DEFAULT_CREATORS {
        let posts = seed
            .posts
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        out.insert(
            seed.name.to_string(),
            CreatorConfig {
                user_id: seed.user_id.to_string(),
                posts,
            },
        );
    }
    serde_json::to_string_pretty(&out).unwrap_or_else(|_| "{}".to_string())
}

pub fn load_users_posts(path: &Path) -> Result<UsersPostsConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file {}", path.display()))?;
    let config: UsersPostsConfig = serde_json::from_str(&content)
        .with_context(|| format!("Invalid JSON in {}", path.display()))?;
    Ok(config)
}
