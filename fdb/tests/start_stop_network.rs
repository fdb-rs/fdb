#[test]
fn start_stop_network() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
        fdb::stop_network();
    }
}
