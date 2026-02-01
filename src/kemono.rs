use anyhow::{Context, Result};
use log::{debug, warn};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, COOKIE, REFERER, USER_AGENT};
use serde::Deserialize;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::rate_limit::RateLimiter;
#[derive(Debug, Clone, Deserialize)]
pub struct Post {
    pub id: String,
    pub title: Option<String>,
    pub published: Option<String>,
    pub attachments: Option<Vec<Attachment>>,
    pub file: Option<Attachment>,
    pub server: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostEnvelope {
    pub post: Post,
    pub attachments: Option<Vec<Attachment>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Attachment {
    pub name: Option<String>,
    pub path: Option<String>,
    pub sha256: Option<String>,
    pub size: Option<i64>,
    pub server: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PostSummary {
    pub id: String,
    pub title: Option<String>,
    pub published: Option<String>,
}

pub fn build_client(base_url: &str, cookies_path: Option<&Path>) -> Result<Client> {
    let mut headers = HeaderMap::new();
    let ua = format!(
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
         (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 czepeku/{}",
        env!("CARGO_PKG_VERSION")
    );
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(&ua).context("Invalid user-agent header")?,
    );
    headers.insert(ACCEPT, HeaderValue::from_static("text/css"));
    headers.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static("en-US,en;q=0.9"),
    );
    if let Ok(referer) = HeaderValue::from_str(base_url) {
        headers.insert(REFERER, referer);
    }

    if let Some(path) = cookies_path {
        debug!("Using cookies file {}", path.display());
        if let Some(cookie_header) = load_cookie_header(path, base_url)? {
            headers.insert(COOKIE, HeaderValue::from_str(&cookie_header)?);
        }
    } else {
        debug!("No cookies configured");
    }

    let client = Client::builder()
        .default_headers(headers)
        .build()
        .context("Failed to build HTTP client")?;

    Ok(client)
}

pub fn fetch_post(
    client: &Client,
    base_url: &str,
    service: &str,
    user_id: &str,
    post_id: &str,
    rate_limiter: Option<&RateLimiter>,
) -> Result<PostEnvelope> {
    if let Some(limiter) = rate_limiter {
        limiter.wait();
    }
    let url = format!(
        "{}/api/v1/{}/user/{}/post/{}",
        trim_slash(base_url),
        service,
        user_id,
        post_id
    );

    let response = client
        .get(&url)
        .send()
        .context("Failed to fetch post")?;
    let status = response.status();
    let text = response.text().context("Failed to read post response")?;
    if !status.is_success() {
        warn!(
            "Post request failed: {} {} body={}",
            status.as_u16(),
            url,
            truncate(&text, 800)
        );
        return Err(anyhow::anyhow!("Post request failed: HTTP {}", status.as_u16()));
    }
    if let Ok(envelope) = serde_json::from_str::<PostEnvelope>(&text) {
        debug!("Fetched post {} ({} bytes)", post_id, text.len());
        return Ok(envelope);
    }

    if let Ok(post) = serde_json::from_str::<Post>(&text) {
        debug!("Fetched post {} ({} bytes, legacy)", post_id, text.len());
        return Ok(PostEnvelope {
            post,
            attachments: None,
        });
    }

    Err(anyhow::anyhow!(
        "Failed to parse post JSON ({} bytes): {}",
        text.len(),
        truncate(&text, 800)
    ))
}

pub fn list_posts(
    client: &Client,
    base_url: &str,
    service: &str,
    user_id: &str,
    rate_limiter: Option<&RateLimiter>,
) -> Result<Vec<PostSummary>> {
    let mut posts = Vec::new();
    let mut offset = 0;

    loop {
        let url = format!(
            "{}/api/v1/{}/user/{}?o={}",
            trim_slash(base_url),
            service,
            user_id,
            offset
        );

        if let Some(limiter) = rate_limiter {
            limiter.wait();
        }

        let response = client
            .get(&url)
            .send()
            .context("Failed to fetch posts list")?;
        let status = response.status();
        let text = response.text().context("Failed to read posts list response")?;
        if !status.is_success() {
            warn!(
                "Posts list request failed: {} {} body={}",
                status.as_u16(),
                url,
                truncate(&text, 800)
            );
            return Err(anyhow::anyhow!("Posts list request failed: HTTP {}", status.as_u16()));
        }
        let batch: Vec<PostSummary> = serde_json::from_str(&text).with_context(|| {
            format!(
                "Failed to parse posts list JSON ({} bytes): {}",
                text.len(),
                truncate(&text, 800)
            )
        })?;

        let count = batch.len();
        posts.extend(batch);
        if count == 0 {
            break;
        }
        offset += count;
    }

    Ok(posts)
}

fn trim_slash(base: &str) -> &str {
    base.trim_end_matches('/')
}

fn load_cookie_header(path: &Path, base_url: &str) -> Result<Option<String>> {
    if !path.exists() {
        warn!("Cookie file not found: {}", path.display());
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read cookie file {}", path.display()))?;
    let host = base_url
        .trim_end_matches('/')
        .split("//")
        .nth(1)
        .unwrap_or(base_url)
        .split('/')
        .next()
        .unwrap_or(base_url)
        .to_lowercase();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let mut cookies = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut fields = line.split('\t');
        let mut domain = fields.next().unwrap_or("").to_string();
        if domain.starts_with("#HttpOnly_") {
            domain = domain.trim_start_matches("#HttpOnly_").to_string();
        }

        let _flag = fields.next();
        let _path = fields.next();
        let _secure = fields.next();
        let expires = fields
            .next()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        let name = fields.next().unwrap_or("");
        let value = fields.next().unwrap_or("");

        if name.is_empty() {
            continue;
        }

        if expires != 0 && expires < now {
            continue;
        }

        let domain = domain.trim_start_matches('.').to_lowercase();
        if host == domain || host.ends_with(&format!(".{}", domain)) {
            cookies.push(format!("{}={}", name, value));
        }
    }

    if cookies.is_empty() {
        Ok(None)
    } else {
        debug!("Loaded {} cookies for {}", cookies.len(), host);
        Ok(Some(cookies.join("; ")))
    }
}

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        return value.to_string();
    }
    let mut out = value.chars().take(max).collect::<String>();
    out.push_str("...");
    out
}
