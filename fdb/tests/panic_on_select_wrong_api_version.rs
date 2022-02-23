#[test]
#[should_panic]
fn panic_on_select_wrong_api_version() {
    unsafe {
        fdb::select_api_version(i32::MAX);
    }
}
