use czepeku::{config, kemono};

fn resolve_post_id() -> Option<(String, String)> {
    let json = config::default_creators_json();
    let cfg: config::UsersPostsConfig = serde_json::from_str(&json).ok()?;
    let creator = cfg.get("czepeku")?;
    if let Some(main) = creator.posts.get("main") {
        return Some((creator.user_id.clone(), main.clone()));
    }
    creator
        .posts
        .values()
        .next()
        .map(|id| (creator.user_id.clone(), id.clone()))
}

#[test]
#[ignore]
fn fetch_post_real_network() -> anyhow::Result<()> {
    let base_url =
        std::env::var("CZEPEKU_ONLINE_BASE_URL").unwrap_or_else(|_| "https://kemono.cr".to_string());
    let service =
        std::env::var("CZEPEKU_ONLINE_SERVICE").unwrap_or_else(|_| "patreon".to_string());
    let user_id = std::env::var("CZEPEKU_ONLINE_USER_ID").ok();
    let post_id = std::env::var("CZEPEKU_ONLINE_POST_ID").ok();

    let (user_id, post_id) = match (user_id, post_id) {
        (Some(u), Some(p)) => (u, p),
        _ => resolve_post_id().expect("Missing default creator data and no env overrides"),
    };

    let client = kemono::build_client(&base_url, None)?;
    let envelope = kemono::fetch_post(&client, &base_url, &service, &user_id, &post_id, None)?;
    assert!(!envelope.post.id.is_empty());
    Ok(())
}
