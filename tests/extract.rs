mod support;

use czepeku::extract::index_extracted_files;
use std::fs::{self, File};
use std::io::Write;

#[test]
fn index_extracted_files_skips_junk() {
    let dir = support::TempDir::new("extract");
    let macosx = dir.path().join("__MACOSX");
    fs::create_dir_all(&macosx).unwrap();
    File::create(macosx.join("._junk")).unwrap();
    File::create(dir.path().join(".DS_Store")).unwrap();
    let map_dir = dir.path().join("Map");
    fs::create_dir_all(&map_dir).unwrap();
    let mut file = File::create(map_dir.join("file.txt")).unwrap();
    file.write_all(b"hello").unwrap();

    let files = index_extracted_files(dir.path()).expect("index");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].rel_path, "Map/file.txt");
}
