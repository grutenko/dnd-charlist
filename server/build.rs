use tonic_build::configure;

fn main()
{
    configure()
        .build_client(false)
        .compile(&["storage.proto"], &["../resources/proto"])
        .unwrap();

    println!("cargo:rerun-if-changed=../resources/proto/storage.proto");
    println!("cargo:rerun-if-changed=build.rs");
}
