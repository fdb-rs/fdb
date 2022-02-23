use fdb::NetworkOption;

#[test]
fn set_network_option() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::set_network_option(NetworkOption::ExternalClientDirectory(String::from(
            "/home/montavista/fdb/fdb-client-lib",
        )))
        .unwrap();
    }
}
