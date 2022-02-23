#[test]
fn select_api_version_once() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
    }
}
