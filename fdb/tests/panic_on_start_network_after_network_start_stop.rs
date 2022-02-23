#[test]
#[should_panic]
fn panic_on_start_network_after_network_start_stop() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);

        fdb::start_network();
        fdb::stop_network();

        fdb::start_network();
    }
}
