use std::{env, fs, path::PathBuf};

fn main() {
    let source = PathBuf::from(env::var_os("GIGATOKEN_SOURCE").expect("GIGATOKEN_SOURCE must point to the pinned 0.10.0 source tree"));
    let manifest = fs::read_to_string(source.join("Cargo.toml")).expect("read GigaToken Cargo.toml");
    assert!(manifest.contains("name = \"gigatoken\"\nversion = \"0.10.0\""), "GigaToken 0.10.0 source required");
    let source = source.canonicalize().expect("canonical GigaToken source").join("src");
    println!("cargo:rerun-if-changed={}", source.display());
    let modules = ["batch.rs", "bpe/mod.rs", "bpe_train.rs", "input/mod.rs", "load_tokenizer/mod.rs", "pretokenize/mod.rs", "token.rs"];
    let mut output = String::new();
    for module in modules {
        let name = module.split('/').next().unwrap().trim_end_matches(".rs");
        let path = source.join(module).canonicalize().expect("GigaToken module");
        output.push_str(&format!("#[path = {:?}] mod {name};\n", path));
        println!("cargo:rerun-if-changed={}", source.join(module).display());
    }
    fs::write(PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("gigatoken_modules.rs"), output).expect("write module manifest");
    println!("cargo:rerun-if-env-changed=GIGATOKEN_SOURCE");
}
