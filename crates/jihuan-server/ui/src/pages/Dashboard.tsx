import { useEffect, useState } from 'react'
import { Files, Database, HardDrive, Percent, RefreshCw, Clock, Layers } from 'lucide-react'
import { getStatus, listBlocks, type StatusResponse, type BlockListResponse } from '@/api'
import { formatBytes, formatDuration } from '@/lib/utils'

interface StatCardProps {
  label: string
  value: string | number
  icon: React.ReactNode
  sub?: string
}

function StatCard({ label, value, icon, sub }: StatCardProps) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5 flex items-start gap-4">
      <div className="bg-indigo-50 text-indigo-600 rounded-lg p-2.5">{icon}</div>
      <div>
        <div className="text-2xl font-bold text-gray-900">{value}</div>
        <div className="text-sm text-gray-500">{label}</div>
        {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
      </div>
    </div>
  )
}

export default function Dashboard() {
  const [status, setStatus] = useState<StatusResponse | null>(null)
  const [blocks, setBlocks] = useState<BlockListResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const [s, b] = await Promise.all([getStatus(), listBlocks()])
      setStatus(s)
      setBlocks(b)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const logicalBytes = status?.logical_bytes ?? 0
  const diskBytes = status?.disk_usage_bytes ?? 0
  // When /api/status is not yet loaded, fall back to summing per-block sizes
  // so the dashboard is never blank between refreshes.
  const totalBlockSize = blocks?.blocks.reduce((acc, b) => acc + b.size, 0) ?? 0
  const displayDisk = diskBytes > 0 ? diskBytes : totalBlockSize
  // Headroom = how much bigger raw uploads are than what's actually on disk.
  // Can be negative on tiny datasets where block overhead dominates; we clip
  // to >= 0 for display but annotate the case below.
  const saved = logicalBytes - displayDisk

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          刷新
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200">
          {error}
        </div>
      )}

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
        <StatCard
          label="文件数"
          value={status?.file_count ?? '—'}
          icon={<Files size={20} />}
        />
        <StatCard
          label="Block 数"
          value={status?.block_count ?? '—'}
          icon={<Database size={20} />}
        />
        <StatCard
          label="文件逻辑总大小"
          value={formatBytes(logicalBytes)}
          icon={<Files size={20} />}
          sub="上传的原始字节（压缩/去重前）"
        />
        <StatCard
          label="实际磁盘占用"
          value={formatBytes(displayDisk)}
          icon={<HardDrive size={20} />}
          sub={
            diskBytes === 0 && totalBlockSize > 0
              ? '（含未 seal）'
              : saved > 0
                ? `节省 ${formatBytes(saved)}`
                : saved < 0
                  ? '< 文件大小：块开销占主导'
                  : '含 block 头/尾 + 压缩'
          }
        />
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="运行时长"
          value={status ? formatDuration(status.uptime_secs) : '—'}
          icon={<Clock size={20} />}
        />
        <StatCard
          label="哈希算法"
          value={status?.hash_algorithm?.toUpperCase() ?? '—'}
          icon={<Percent size={20} />}
          sub={`压缩: ${status?.compression_algorithm ?? '—'} L${status?.compression_level ?? ''}`}
        />
        <StatCard
          label="去重比"
          value={status ? `${status.dedup_ratio.toFixed(2)}x` : '—'}
          icon={<Layers size={20} />}
          sub={
            status && status.dedup_ratio > 1
              ? `节省 ${((1 - 1 / status.dedup_ratio) * 100).toFixed(1)}%`
              : '逻辑 / 实际'
          }
        />
        <StatCard
          label="Active / Sealed"
          value={blocks ? `${blocks.blocks.filter(b => !b.sealed).length} / ${blocks.blocks.filter(b => b.sealed).length}` : '—'}
          icon={<Layers size={20} />}
          sub="未 seal / 已 seal"
        />
      </div>

      {status && status.max_storage_bytes != null && (() => {
        const cap = status.max_storage_bytes!
        const used = displayDisk
        const pct = cap > 0 ? Math.min(100, (used / cap) * 100) : 0
        // Color switches based on fill: green < 70%, amber < 90%, red otherwise.
        const barColor = pct >= 90 ? 'bg-red-500' : pct >= 70 ? 'bg-amber-500' : 'bg-emerald-500'
        return (
          <div className="bg-white rounded-xl border border-gray-200 p-5 mb-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-base font-semibold text-gray-800">存储配额</h2>
              <span className="text-xs text-gray-500 font-mono">
                {formatBytes(used)} / {formatBytes(cap)} ({pct.toFixed(1)}%)
              </span>
            </div>
            <div className="h-2.5 bg-gray-100 rounded-full overflow-hidden">
              <div
                className={`h-full ${barColor} rounded-full transition-all`}
                style={{ width: `${pct}%` }}
              />
            </div>
            {pct >= 90 && (
              <p className="mt-2 text-xs text-red-600">
                已接近上限，新上传将被拒绝并返回 507 Insufficient Storage。
              </p>
            )}
          </div>
        )
      })()}

      {status && status.max_storage_bytes == null && (
        <div className="bg-white rounded-xl border border-gray-200 p-4 mb-4 text-xs text-gray-500">
          <span className="font-semibold text-gray-700">存储配额：</span>未配置（无限制）。
          可在 <code className="font-mono">config/*.toml</code> 的
          <code className="font-mono">[storage]</code> 下设置
          <code className="font-mono"> max_storage_bytes = …</code>
        </div>
      )}

      {status && (
        <div className="bg-white rounded-xl border border-gray-200 p-5">
          <h2 className="text-base font-semibold text-gray-800 mb-4">系统信息</h2>
          <div className="grid grid-cols-2 gap-3 text-sm">
            {[
              ['版本', status.version],
              ['哈希算法', status.hash_algorithm],
              ['压缩算法', status.compression_algorithm],
              ['压缩级别', String(status.compression_level)],
            ].map(([k, v]) => (
              <div key={k} className="flex items-center justify-between py-2 border-b border-gray-100 last:border-0">
                <span className="text-gray-500">{k}</span>
                <span className="font-mono text-gray-800">{v}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {blocks && blocks.blocks.length > 0 && (
        <div className="mt-4 bg-white rounded-xl border border-gray-200 p-5">
          <h2 className="text-base font-semibold text-gray-800 mb-4">Block 分布（前 10）</h2>
          <div className="space-y-2">
            {blocks.blocks.slice(0, 10).map((b) => {
              const pct = displayDisk > 0 ? (b.size / displayDisk) * 100 : 0
              return (
                <div key={b.block_id}>
                  <div className="flex justify-between text-xs text-gray-500 mb-1">
                    <span className="font-mono truncate max-w-xs">{b.block_id.slice(0, 16)}…</span>
                    <span>{formatBytes(b.size)}</span>
                  </div>
                  <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-indigo-400 rounded-full"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
