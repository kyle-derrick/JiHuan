# One-shot release-mode smoke orchestrator.
#
# 1. Writes a temporary config with auth disabled + unique dirs
# 2. Launches target\release\jihuan-server.exe in the background
# 3. Runs tests/perf/smoke_e2e.py against it
# 4. Stops the server and deletes the temp dir
#
# Exits 0 iff smoke_e2e.py exited 0.
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path $PSScriptRoot -Parent | Split-Path -Parent
$exe      = Join-Path $repoRoot "target\release\jihuan-server.exe"
if (-not (Test-Path $exe)) {
    Write-Error "missing $exe — run cargo build --release first"
    exit 2
}

$workDir   = Join-Path $env:TEMP ("jh-smoke-" + [Guid]::NewGuid().ToString("N").Substring(0, 8))
$dataDir   = Join-Path $workDir "data"
$metaDir   = Join-Path $workDir "meta"
$walDir    = Join-Path $workDir "wal"
$configPath = Join-Path $workDir "config.toml"
New-Item -ItemType Directory -Force -Path $workDir, $dataDir, $metaDir, $walDir | Out-Null

# Release-mode smoke config: auth off so we don't need bootstrap key juggling,
# dedicated port to avoid clashing with any dev server.
@"
[storage]
data_dir = "$($dataDir -replace '\\','/')"
meta_dir = "$($metaDir -replace '\\','/')"
wal_dir  = "$($walDir -replace '\\','/')"
block_file_size = 67108864
chunk_size = 4194304
hash_algorithm = "sha256"
compression_algorithm = "zstd"
compression_level = 1
time_partition_hours = 24
gc_threshold = 0.7
gc_interval_secs = 300
max_open_block_files = 16
verify_on_read = true

[server]
http_addr = "127.0.0.1:18080"
grpc_addr = "127.0.0.1:18081"
metrics_addr = "127.0.0.1:19090"
enable_access_log = false
cors_origins = []

[auth]
enabled = false
exempt_routes = ["/"]
audit_retention_days = 1
"@ | Set-Content -Path $configPath -Encoding UTF8

Write-Host "smoke workdir : $workDir"
Write-Host "smoke config  : $configPath"

$logOut = Join-Path $workDir "server.out.log"
$logErr = Join-Path $workDir "server.err.log"

$env:JIHUAN_CONFIG = $configPath
$proc = Start-Process -FilePath $exe `
    -RedirectStandardOutput $logOut `
    -RedirectStandardError  $logErr `
    -PassThru `
    -WindowStyle Hidden

Write-Host "launched jihuan-server pid=$($proc.Id)"
$smokeExit = 1
try {
    $env:JIHUAN_BASE = "http://127.0.0.1:18080"
    $env:JIHUAN_ADMIN_KEY = "unused-auth-off"
    & python (Join-Path $PSScriptRoot "smoke_e2e.py")
    $smokeExit = $LASTEXITCODE
} finally {
    if (-not $proc.HasExited) {
        Write-Host "stopping server..."
        try { Stop-Process -Id $proc.Id -Force } catch { }
    }
    Start-Sleep -Milliseconds 500
    Write-Host ""
    Write-Host "--- server.err tail ---"
    if (Test-Path $logErr) {
        Get-Content $logErr -Tail 20 | ForEach-Object { Write-Host "  $_" }
    }
    # Keep the dir on failure for post-mortem, delete on success.
    if ($smokeExit -eq 0) {
        Remove-Item -Recurse -Force $workDir -ErrorAction SilentlyContinue
    } else {
        Write-Host ""
        Write-Host "workdir retained at $workDir"
    }
}
exit $smokeExit
