# JiHuan — 高性能小文件存储系统

> 用 Rust 编写的生产级、追加写入小文件存储系统，具备全局去重、透明压缩和时间分区生命周期管理。

---

## 目录

1. [特性](#特性)
2. [架构](#架构)
3. [编译与打包](#编译与打包)
4. [启动服务](#启动服务)
5. [CLI 使用](#cli-使用)
6. [HTTP API](#http-api)
7. [gRPC API](#grpc-api)
8. [配置说明](#配置说明)
9. [推荐配置场景](#推荐配置场景)
10. [监控指标](#监控指标)
11. [运行测试](#运行测试)
12. [项目结构](#项目结构)

---

## 特性

- **全局内容去重** — SHA-256/SHA-1/MD5 内容哈希；相同数据块只存一份
- **透明压缩** — LZ4（速度优先）或 Zstd（压缩率优先），压缩级别可配置
- **时间分区生命周期** — 按时间窗口自动 GC，支持冷热数据分离
- **写前日志（WAL）** — 崩溃恢复，异常关机不丢数据
- **双协议 API** — HTTP/1.1+HTTP/2 RESTful（Axum :8080）和 gRPC（Tonic :8081）
- **CLI 工具** — `jihuan put/get/delete/stat/status/gc/list-blocks`
- **数据完整性校验** — 每个 chunk 有独立 CRC32 + 内容哈希，读时可验证
- **Prometheus 监控** — 内置 metrics 端点（:9090），开箱即用
- **5 种配置模板** — 通用、极速、极压、小文件、大文件

---

## 架构

```
┌──────────────────────────────────────────────────────┐
│                    API Layer                         │
│         HTTP (axum :8080)  |  gRPC (tonic :8081)    │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│                  Engine (jihuan-core)                │
│  put_bytes / get_bytes / delete_file / trigger_gc   │
└──┬──────────────┬──────────────┬────────────────┬────┘
   │              │              │                │
   ▼              ▼              ▼                ▼
Chunker      Compressor       Dedup          MetadataStore
(固定大小)   (LZ4/Zstd/None) (哈希索引)     (redb KV)
   │                           │
   └───────────────────────────┘
              │
   ┌──────────▼──────────┐
   │   Block Files       │   ← 磁盘上的追加写入 .blk 文件
   │  (writer/reader)    │   ← 活跃块数据常驻内存缓存
   └─────────────────────┘
              │
   ┌──────────▼──────────┐
   │   WAL + GC          │   ← 崩溃恢复 + 分区清理
   └─────────────────────┘
```

---

## 编译与打包

### 环境要求

| 依赖 | 版本 | 说明 |
|------|------|------|
| Rust | ≥ 1.80 | 安装：`curl https://sh.rustup.rs -sSf \| sh` |
| protoc | ≥ 3.15 | gRPC 代码生成，Linux: `apt install protobuf-compiler` |

### 标准 Release 构建

```bash
# 编译 server + CLI（推荐，thin-LTO，最优二进制大小和性能）
cargo build --release -p jihuan-server -p jihuan-cli

# 产物路径
./target/release/jihuan-server
./target/release/jihuan
```

### 极限性能构建（full LTO，编译慢约 3×，运行更快）

```bash
cargo build --profile release-full-lto -p jihuan-server -p jihuan-cli
./target/release-full-lto/jihuan-server
```

### 带调试信息的 Release（用于 perf / flamegraph 性能分析）

```bash
cargo build --profile profiling -p jihuan-server
./target/profiling/jihuan-server
```

### Docker 打包

```bash
# 构建镜像
docker build -f scripts/Dockerfile -t jihuan-server:latest .

# 或使用 docker-compose（含挂载卷 + 健康检查）
docker-compose -f scripts/docker-compose.yml up -d
```

> **注意**：Docker 镜像使用多阶段构建，最终镜像只含二进制，体积约 20 MB。

---

## 启动服务

### 最简启动（使用内置默认配置）

```bash
./target/release/jihuan-server --data-dir ./jihuan-data
```

### 使用配置文件启动

```bash
./target/release/jihuan-server --config config/default.toml
```

### 覆盖单个配置项（命令行优先于配置文件）

```bash
./target/release/jihuan-server \
  --config config/default.toml \
  --data-dir /mnt/nvme/jihuan \
  --http-addr 0.0.0.0:9000 \
  --grpc-addr 0.0.0.0:9001
```

### 环境变量

所有命令行参数均有对应的环境变量，适合容器部署：

| 环境变量 | 对应参数 | 示例 |
|---------|---------|------|
| `JIHUAN_CONFIG` | `--config` | `/app/config/default.toml` |
| `JIHUAN_DATA_DIR` | `--data-dir` | `/data` |
| `JIHUAN_HTTP_ADDR` | `--http-addr` | `0.0.0.0:8080` |
| `JIHUAN_GRPC_ADDR` | `--grpc-addr` | `0.0.0.0:8081` |
| `RUST_LOG` | 日志级别 | `info` / `debug` / `warn` |

```bash
JIHUAN_CONFIG=/app/config/default.toml \
RUST_LOG=info \
./jihuan-server
```

---

## CLI 使用

```bash
# 设置 CLI 数据目录（与服务器数据目录一致）
export JIHUAN_DATA_DIR=./jihuan-data

# 上传文件（返回 file_id）
jihuan put photo.jpg
jihuan put report.pdf --name "2024年报.pdf" --content-type application/pdf

# 下载文件
jihuan get <file_id>                        # 输出到 stdout
jihuan get <file_id> --output out.jpg       # 保存到文件

# 查看文件元数据
jihuan stat <file_id>

# 删除文件
jihuan delete <file_id>

# 查看系统状态（文件数、块数、磁盘用量、去重率）
jihuan status

# 手动触发垃圾回收
jihuan gc

# 列出所有块文件
jihuan list-blocks

# 验证配置文件
jihuan validate-config --config config/default.toml
```

---

## HTTP API

### 上传文件

```bash
curl -X POST http://localhost:8080/api/v1/files \
  -F "file=@photo.jpg"
# 返回: {"file_id":"...","file_name":"photo.jpg","file_size":12345}
```

### 下载文件

```bash
curl http://localhost:8080/api/v1/files/<file_id> --output out.jpg
```

### 查看元数据

```bash
curl http://localhost:8080/api/v1/files/<file_id>/meta
```

### 删除文件

```bash
curl -X DELETE http://localhost:8080/api/v1/files/<file_id>
```

### 系统状态

```bash
curl http://localhost:8080/api/status
```

### 触发 GC

```bash
curl -X POST http://localhost:8080/api/gc/trigger
```

### 完整端点表

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/files` | 上传文件（multipart/form-data） |
| `GET` | `/api/v1/files/:id` | 下载文件 |
| `DELETE` | `/api/v1/files/:id` | 删除文件 |
| `GET` | `/api/v1/files/:id/meta` | 文件元数据 |
| `GET` | `/api/status` | 系统状态 |
| `POST` | `/api/gc/trigger` | 手动触发 GC |
| `GET` | `/api/block/list` | 列出所有块文件 |

---

## gRPC API

Proto 文件：`crates/jihuan-server/proto/jihuan.proto`

```bash
# 使用 grpcurl 测试
grpcurl -plaintext localhost:8081 list
grpcurl -plaintext -d '{"file_name":"test.txt","data":"aGVsbG8="}' \
  localhost:8081 jihuan.FileService/PutFile
```

**认证**：gRPC 通道仅接受 API Key（`authorization: Bearer <key>` metadata）。
浏览器 session cookie 是 HTTP-only 特性，gRPC 侧不会支持 —— 需要自动
续期的客户端请改用 `POST /api/auth/keys` 申请长期 Service Account。

---

## 配置说明

配置文件为 TOML 格式，分两个 section：`[storage]` 和 `[server]`。

### `[storage]` 存储配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `data_dir` | 路径 | 必填 | 块文件存储目录 |
| `meta_dir` | 路径 | 必填 | redb 元数据库目录 |
| `wal_dir` | 路径 | 必填 | WAL 日志目录 |
| `block_file_size` | 字节 | `1073741824`（1 GB）| 单个块文件最大大小 |
| `chunk_size` | 字节 | `4194304`（4 MB）| 分块大小，影响去重粒度 |
| `hash_algorithm` | 枚举 | `sha256` | `sha256` / `sha1` / `md5` / `none` |
| `compression_algorithm` | 枚举 | `zstd` | `zstd` / `lz4` / `none` |
| `compression_level` | 整数 | `1` | Zstd: 1–22；LZ4: 忽略；none: 0 |
| `time_partition_hours` | 整数 | `24` | 时间分区粒度（小时），决定 GC 最小单位 |
| `gc_threshold` | 浮点 | `0.7` | 块文件引用率低于此值触发 GC（0.0–1.0） |
| `gc_interval_secs` | 整数 | `300` | 后台 GC 扫描间隔（秒） |
| `max_open_block_files` | 整数 | `64` | LRU 缓存的最大已打开块文件数 |
| `verify_on_read` | 布尔 | `true` | 读取时验证 CRC32 + 内容哈希 |

### `[server]` 服务配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `http_addr` | 字符串 | `0.0.0.0:8080` | HTTP 监听地址 |
| `grpc_addr` | 字符串 | `0.0.0.0:8081` | gRPC 监听地址 |
| `metrics_addr` | 字符串 | `0.0.0.0:9090` | Prometheus metrics 端点 |
| `worker_threads` | 整数 | CPU 核心数 | Tokio 工作线程数 |
| `max_body_size` | 字节/null | `null`（无限制）| 最大请求体大小 |
| `enable_access_log` | 布尔 | `true` | 是否开启请求日志 |

---

## 推荐配置场景

### 场景 1：通用生产环境（`config/default.toml`）

**适用**：多种文件大小混合，均衡安全性与性能。

```toml
[storage]
data_dir = "/data/jihuan/data"
meta_dir = "/data/jihuan/meta"
wal_dir  = "/data/jihuan/wal"
block_file_size    = 1073741824   # 1 GB：较大块减少文件数量
chunk_size         = 4194304      # 4 MB：去重粒度适中
hash_algorithm     = "sha256"     # 安全的内容哈希，去重准确
compression_algorithm = "zstd"
compression_level  = 1            # Zstd level 1：压缩率与速度最佳平衡点
time_partition_hours = 24         # 按天分区
gc_threshold       = 0.7          # 70% 引用率以下触发回收
gc_interval_secs   = 300
max_open_block_files = 64
verify_on_read     = true

[server]
http_addr    = "0.0.0.0:8080"
grpc_addr    = "0.0.0.0:8081"
metrics_addr = "0.0.0.0:9090"
enable_access_log = true
```

---

### 场景 2：极速热数据（`config/speed.toml`）

**适用**：缓存层、临时文件、高频读写、延迟敏感场景。  
**特点**：关闭压缩和校验，MD5 哈希开销最小，`verify_on_read = false`。  
⚠️ **注意**：MD5 有碰撞风险，仅适合内部私有系统。

```toml
[storage]
data_dir = "/data/jihuan/data"
meta_dir = "/data/jihuan/meta"
wal_dir  = "/data/jihuan/wal"
block_file_size    = 536870912    # 512 MB：频繁轮转，减少单块大小
chunk_size         = 8388608      # 8 MB：减小分块次数，提升写入吞吐
hash_algorithm     = "md5"        # 最快哈希算法
compression_algorithm = "none"    # 不压缩：节省 CPU，降低延迟
compression_level  = 0
time_partition_hours = 12         # 按半天分区，GC 更激进
gc_threshold       = 0.8
gc_interval_secs   = 120
max_open_block_files = 128        # 更多打开文件，适合高并发读
verify_on_read     = false        # 关闭校验：读取延迟更低

[server]
http_addr    = "0.0.0.0:8080"
grpc_addr    = "0.0.0.0:8081"
metrics_addr = "0.0.0.0:9090"
enable_access_log = false         # 关闭访问日志减少 I/O 开销
```

---

### 场景 3：极限压缩冷存档（`config/space.toml`）

**适用**：日志归档、备份存储、访问频率极低的冷数据。  
**特点**：Zstd level 9 压缩率高，分区窗口 72 小时减少 GC 频率。

```toml
[storage]
data_dir = "/data/jihuan/data"
meta_dir = "/data/jihuan/meta"
wal_dir  = "/data/jihuan/wal"
block_file_size    = 2147483648   # 2 GB：大块减少文件数和元数据压力
chunk_size         = 4194304      # 4 MB
hash_algorithm     = "sha256"
compression_algorithm = "zstd"
compression_level  = 9            # 高压缩率（写入慢，空间最省）
time_partition_hours = 72         # 按 3 天分区
gc_threshold       = 0.6          # 更激进的 GC，回收空间优先
gc_interval_secs   = 600
max_open_block_files = 32
verify_on_read     = true

[server]
http_addr    = "0.0.0.0:8080"
grpc_addr    = "0.0.0.0:8081"
metrics_addr = "0.0.0.0:9090"
enable_access_log = true
```

---

### 场景 4：小文件密集型（`config/small-files.toml`）

**适用**：图片、缩略图、头像、图标、文档碎片（90% 文件 < 10 KB）。  
**特点**：更小的 `chunk_size` 避免内部碎片，更小的 `block_file_size` 加快 GC。

```toml
[storage]
data_dir = "/data/jihuan/data"
meta_dir = "/data/jihuan/meta"
wal_dir  = "/data/jihuan/wal"
block_file_size    = 536870912    # 512 MB：小块文件加快 GC 回收
chunk_size         = 2097152      # 2 MB：减小分块以匹配小文件大小
hash_algorithm     = "sha256"
compression_algorithm = "zstd"
compression_level  = 3            # level 3：比 level 1 压缩率更好，适合图片以外的文本
time_partition_hours = 24
gc_threshold       = 0.7
gc_interval_secs   = 300
max_open_block_files = 64
verify_on_read     = true

[server]
http_addr    = "0.0.0.0:8080"
grpc_addr    = "0.0.0.0:8081"
metrics_addr = "0.0.0.0:9090"
enable_access_log = true
```

---

### 场景 5：大文件吞吐型（`config/large-files.toml`）

**适用**：视频、数据集、ISO 镜像、数据库备份（90% 文件 > 100 MB）。  
**特点**：更大的 `chunk_size` 和 `block_file_size` 提升顺序写入吞吐。

```toml
[storage]
data_dir = "/data/jihuan/data"
meta_dir = "/data/jihuan/meta"
wal_dir  = "/data/jihuan/wal"
block_file_size    = 2147483648   # 2 GB：减少块文件个数和元数据压力
chunk_size         = 8388608      # 8 MB：大 chunk 减少索引项，提升写入带宽
hash_algorithm     = "sha256"
compression_algorithm = "zstd"
compression_level  = 1            # level 1：对视频/已压缩数据几乎无效益，保持速度
time_partition_hours = 72
gc_threshold       = 0.8
gc_interval_secs   = 600
max_open_block_files = 32
verify_on_read     = true

[server]
http_addr    = "0.0.0.0:8080"
grpc_addr    = "0.0.0.0:8081"
metrics_addr = "0.0.0.0:9090"
enable_access_log = true
```

---

### 配置场景对比速查

| | 通用 | 极速 | 极压 | 小文件 | 大文件 |
|---|---|---|---|---|---|
| **文件** | `default.toml` | `speed.toml` | `space.toml` | `small-files.toml` | `large-files.toml` |
| **块大小** | 1 GB | 512 MB | 2 GB | 512 MB | 2 GB |
| **分块大小** | 4 MB | 8 MB | 4 MB | 2 MB | 8 MB |
| **哈希** | SHA-256 | MD5 | SHA-256 | SHA-256 | SHA-256 |
| **压缩** | Zstd-1 | 无 | Zstd-9 | Zstd-3 | Zstd-1 |
| **读取校验** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **分区窗口** | 24 h | 12 h | 72 h | 24 h | 72 h |
| **适用场景** | 混合工作负载 | 缓存/低延迟 | 冷归档 | 图片/缩略图 | 视频/备份 |

---

## 监控指标

服务启动后，Prometheus 指标默认暴露在 `:9090`：

```bash
curl http://localhost:9090/metrics
```

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `jihuan_puts_total` | Counter | 累计 put 操作次数 |
| `jihuan_gets_total` | Counter | 累计 get 操作次数 |
| `jihuan_deletes_total` | Counter | 累计 delete 操作次数 |
| `jihuan_dedup_hits_total` | Counter | 累计去重命中次数 |
| `jihuan_bytes_written_total` | Counter | 累计写入字节数 |
| `jihuan_bytes_read_total` | Counter | 累计读取字节数 |
| `jihuan_put_duration_seconds` | Histogram | put 操作延迟分布 |
| `jihuan_get_duration_seconds` | Histogram | get 操作延迟分布 |

---

## 运行测试

```bash
# 全部单元 + 集成测试（推荐 CI 使用）
cargo test --workspace --exclude jihuan-server --exclude jihuan-bench

# 仅 core 库
cargo test -p jihuan-core

# 仅集成测试
cargo test -p jihuan-core --test integration_engine

# 混沌 / 压力测试
cargo test -p jihuan-tests

# 性能基准测试
cargo bench -p jihuan-bench
```

---

## 项目结构

```
JiHuan/
├── Cargo.toml                    # Workspace 根配置
├── config/                       # 配置模板
│   ├── default.toml              # 通用生产配置
│   ├── speed.toml                # 极速热数据
│   ├── space.toml                # 极压冷存档
│   ├── small-files.toml          # 小文件密集型
│   └── large-files.toml          # 大文件吞吐型
├── scripts/
│   ├── docker-build.sh           # Docker 构建脚本
│   └── docker-compose.yml        # 容器编排（含挂载卷 + 健康检查）
├── crates/
│   ├── jihuan-core/              # 存储引擎核心库
│   │   └── src/
│   │       ├── block/            # 块文件格式、写入器、读取器
│   │       ├── chunking/         # 固定大小分块
│   │       ├── compression/      # LZ4 / Zstd
│   │       ├── config/           # AppConfig + 配置模板
│   │       ├── dedup/            # 内容哈希 + CRC32
│   │       ├── error/            # JiHuanError + 重试逻辑
│   │       ├── gc/               # 后台 GC + 崩溃恢复
│   │       ├── metadata/         # redb KV 元数据存储
│   │       ├── metrics/          # EngineStats 快照
│   │       ├── utils/            # 时间、UUID、磁盘工具
│   │       ├── wal/              # 写前日志
│   │       └── engine.rs         # Engine 顶层 API
│   ├── jihuan-server/            # HTTP + gRPC 服务器二进制
│   │   └── proto/jihuan.proto    # Protobuf 服务定义
│   ├── jihuan-cli/               # CLI 二进制
│   └── jihuan-bench/             # Criterion 基准测试
└── tests/                        # 混沌 / 压力集成测试
```

---

## License

MIT