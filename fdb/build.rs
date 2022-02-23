use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

fn main() {
    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is undefined!"));
    let option_file = out_path.join("option.rs");
    let mut options = String::new();
    fdb_gen::emit(&mut options).expect("couldn't emit options.rs code!");

    File::create(option_file)
        .expect("couldn't create option.rs!")
        .write_all(options.as_bytes())
        .expect("couldn't write option.rs!");
}
