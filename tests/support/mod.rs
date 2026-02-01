use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(prefix: &str) -> Self {
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

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
