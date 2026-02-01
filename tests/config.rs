use czepeku::config::{default_creators_json, UsersPostsConfig};

#[test]
fn default_creators_json_is_valid() {
    let json = default_creators_json();
    let config: UsersPostsConfig = serde_json::from_str(&json).expect("default json parse");
    assert!(config.contains_key("czepeku"));
    let czepeku = config.get("czepeku").expect("czepeku entry");
    assert_eq!(czepeku.user_id, "16010661");
    assert!(czepeku.posts.contains_key("main"));
}
