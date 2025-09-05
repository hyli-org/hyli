// Deactivate in clippy, we don't really need to recompile and it breaks our CI.
#[cfg(any(clippy, not(feature = "build")))]
fn main() {}

#[cfg(all(
    feature = "build",
    not(any(
        feature = "amm",
        feature = "hydentity",
        feature = "hyllar",
        feature = "smt-token",
        feature = "staking",
        feature = "risc0-recursion",
        feature = "uuid-tld"
    ))
))]
fn main() {
    compile_error!("When the 'build' feature is enabled, at least one of the following features must also be enabled: all, amm, hydentity, hyllar, smt-token, staking, risc0-recursion, uuid-tld.");
}

#[cfg(all(
    not(clippy),
    feature = "build",
    any(
        feature = "amm",
        feature = "hydentity",
        feature = "hyllar",
        feature = "smt-token",
        feature = "staking",
        feature = "risc0-recursion",
        feature = "uuid-tld"
    )
))]
fn main() {
    trait CodegenConsts {
        fn codegen_consts(&self) -> String;
    }

    impl CodegenConsts for risc0_build::GuestListEntry {
        fn codegen_consts(&self) -> String {
            use std::fmt::Write;
            // Quick check for '#' to avoid injection of arbitrary Rust code into the
            // method.rs file. This would not be a serious issue since it would only
            // affect the user that set the path, but it's good to add a check.
            if self.path.contains('#') {
                panic!("method path cannot include #: {}", self.path);
            }

            let upper = self.name.to_uppercase().replace('-', "_");

            let image_id = self.image_id.as_words();
            let elf = format!("include_bytes!({:?})", self.path);

            let mut str = String::new();

            writeln!(&mut str, "pub const {upper}_ELF: &[u8] = {elf};").unwrap();
            writeln!(&mut str, "pub const {upper}_PATH: &str = {:?};", self.path).unwrap();
            writeln!(&mut str, "pub const {upper}_ID: [u32; 8] = {image_id:?};").unwrap();

            str
        }
    }

    // clippy in workspace mode sets this, which interferes with the guest VM. Clear it temporarily.
    let env_wrapper = std::env::var("RUSTC_WORKSPACE_WRAPPER");
    std::env::set_var("RUSTC_WORKSPACE_WRAPPER", "");

    println!("cargo:rerun-if-changed=build.rs");

    use risc0_build::{
        build_package, get_package, DockerOptionsBuilder, GuestListEntry, GuestOptionsBuilder,
    };
    use std::io::Write;

    let reproducible = cfg!(not(feature = "nonreproducible"));

    let pkg = get_package(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let manifest_dir = pkg.manifest_path.parent().unwrap();

    let methods: Vec<GuestListEntry> = [
        #[cfg(feature = "amm")]
        "amm",
        #[cfg(feature = "hydentity")]
        "hydentity",
        #[cfg(feature = "hyllar")]
        "hyllar",
        #[cfg(feature = "smt-token")]
        "smt-token",
        #[cfg(feature = "staking")]
        "staking",
        #[cfg(feature = "risc0-recursion")]
        "risc0-recursion",
        #[cfg(feature = "uuid-tld")]
        "uuid-tld",
    ]
    .iter()
    .map(|name| {
        let pkg = get_package(manifest_dir.join(name));
        let mut guest_opts = GuestOptionsBuilder::default();

        println!("cargo:rerun-if-changed={name}");
        guest_opts.features(vec!["risc0".into()]);

        if reproducible {
            guest_opts.use_docker(
                DockerOptionsBuilder::default()
                    // Point to the workspace
                    .root_dir("../..".to_string())
                    .build()
                    .unwrap(),
            );
        }

        build_package(
            &pkg,
            std::env::var("OUT_DIR").expect("missing OUT_DIR env var"),
            guest_opts.build().expect("failed to build guest options"),
        )
    })
    .flatten()
    .flatten()
    .collect();

    let out_dir_env = std::env::var_os("OUT_DIR").unwrap();
    let out_dir = std::path::Path::new(&out_dir_env);

    let methods_path = out_dir.join("methods.rs");
    let mut methods_file = std::fs::File::create(&methods_path).unwrap();

    for method in methods.iter() {
        methods_file
            .write_all(method.codegen_consts().as_bytes())
            .unwrap();
    }

    // if reproducible {
    methods.iter().for_each(|data| {
        std::fs::write(format!("{}/{}.img", data.name, data.name), &data.elf)
            .expect("failed to write img");
        // Convert u32 slice to hex
        let hex_image_id = data
            .image_id
            .as_words()
            .iter()
            .map(|x| format!("{:08x}", x.to_be()))
            .collect::<Vec<_>>()
            .join("");
        std::fs::write(format!("{}/{}.txt", data.name, data.name), &hex_image_id)
            .expect("failed to write program ID");
    });
    // }
    std::env::set_var("RUSTC_WORKSPACE_WRAPPER", env_wrapper.unwrap_or_default());
}
