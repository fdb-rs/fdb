fn main() {
    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    // Default cluster file specified by environment variable
    // FDB_CLUSTER_FILE is opened.
    let fdb_database = fdb::open_database("").unwrap();

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }
}
