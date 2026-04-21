fn main() {
    // ── Proto compilation ─────────────────────────────────────────────────────
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/jihuan.proto"], &["proto/"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {}", e));

    // ── UI build ──────────────────────────────────────────────────────────────
    // Rebuild if any source file changes, or if dist/ doesn't exist yet.
    println!("cargo:rerun-if-changed=ui/src");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/vite.config.ts");

    let ui_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("ui");
    let dist_dir = ui_dir.join("dist");

    // Only run npm build if dist/ is missing (CI/first checkout) or src is newer.
    // During dev, devs run `npm run build` manually or via `npm run dev`.
    // Skip build if JIHUAN_SKIP_UI_BUILD env var is set.
    if std::env::var("JIHUAN_SKIP_UI_BUILD").is_ok() {
        return;
    }

    if !dist_dir.exists() {
        println!("cargo:warning=UI dist/ not found, running npm run build...");
        let npm = if cfg!(target_os = "windows") { "npm.cmd" } else { "npm" };
        let status = std::process::Command::new(npm)
            .args(["run", "build"])
            .current_dir(&ui_dir)
            .status()
            .expect("Failed to run npm. Is Node.js installed?");
        if !status.success() {
            panic!("npm run build failed");
        }
    }
}
