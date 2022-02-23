// Make `cargo build` panic with the right message without warnings
// when called without `
#![allow(unreachable_code)]
#![allow(unused_assignments)]
#![allow(unused_variables)]
#![allow(unused_mut)]

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

#[cfg(not(any(feature = "fdb-6_3")))]
const INCLUDE_PATH: &str = "";

#[cfg(feature = "fdb-6_3")]
const INCLUDE_PATH: &str = "-I./include/630";

fn main() {
    // Link against fdb_c.
    println!("cargo:rustc-link-lib=dylib=fdb_c");

    if let Ok(link_search) = env::var("RUSTC_LINK_SEARCH_FDB_CLIENT_LIB") {
        println!("cargo:rustc-link-search=native={}", link_search);
    }

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is not defined!"));

    // We need to have FDB_API_VERSION set to a constant so that
    // bindgen will generate a const value for it. We could try to
    // pass -DFDB_TRICKY_VERSION=630 to the driver and then '#define
    // FDB_API_VERSION FDB_TRICKY_VERSION', but bindgen isn't smart
    // enough to resolve that from the arguments. Instead, write out a
    // src/wrapper.h file with the chosen version instead.
    let mut api_version = 0;

    #[cfg(not(any(feature = "fdb-6_3")))]
    panic!("Please specify fdb-<major>_<minor> feature");

    #[cfg(feature = "fdb-6_3")]
    {
        api_version = 630;
    }

    // Sigh, bindgen only takes a String for its header path, but
    // that's UTF-8 while PathBuf is OS-native...
    let wpath = out_path.join("wrapper.h");
    let wrapper_path = wpath
        .to_str()
        .expect("couldn't convert wrapper PathBuf to String!");

    let mut wrapper = File::create(wrapper_path).expect("couldn't create wrapper.h!");
    wrapper
        .write_all(
            format!(
                "// This is used as `header_version` in\n\
                            // `fdb_select_api_version_impl`\n\
                            #define FDB_API_VERSION {}\n",
                api_version
            )
            .as_bytes(),
        )
        .expect("couldn't write wrapper.h!");
    wrapper
        .write_all(b"#include <fdb_c.h>\n")
        .expect("couldn't write wrapper.h!");
    drop(wrapper);

    // Finish up by writing the actual bindings
    let bindings = bindgen::Builder::default()
        .clang_arg(INCLUDE_PATH)
        .header(wrapper_path)
        .generate_comments(true)
        .generate()
        .expect("Unable to generate FoundationDB bindings");
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
