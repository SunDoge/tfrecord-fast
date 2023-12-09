/// This is not an exmaple, but a tool to generate prost code.

fn main() -> std::io::Result<()> {
    let out_dir = "src/proto";
    std::env::set_var("OUT_DIR", out_dir);
    std::fs::create_dir_all(out_dir)?;
    prost_build::compile_protos(&["tensorflow/core/example/example.proto"], &["."])?;
    Ok(())
}
