fn main() {
    let mut code = String::new();
    fdb_gen::emit(&mut code).expect("couldn't generate options.rs code!");
    println!("{}", code);
}
