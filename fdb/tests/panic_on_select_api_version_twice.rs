#[test]
#[should_panic]
fn panic_on_select_api_version_twice() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
    }
}
