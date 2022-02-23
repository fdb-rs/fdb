#[test]
#[should_panic]
fn panic_on_stop_network_without_start_network() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::stop_network();
    }
}
