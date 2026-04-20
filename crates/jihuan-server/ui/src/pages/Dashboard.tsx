import { useEffect, useState } from 'react'
import { Files, Database, HardDrive, Percent, RefreshCw } from 'lucide-react'
import { getStatus, listBlocks, type StatusResponse, type BlockListResponse } from '@/api'
import { formatBytes } from '@/lib/utils'

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

  const totalDisk = blocks?.blocks.reduce((acc, b) => acc + b.size, 0) ?? 0

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

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
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
          label="磁盘占用"
          value={formatBytes(totalDisk)}
          icon={<HardDrive size={20} />}
        />
        <StatCard
          label="算法"
          value={status?.hash_algorithm?.toUpperCase() ?? '—'}
          icon={<Percent size={20} />}
          sub={`压缩: ${status?.compression_algorithm ?? '—'} L${status?.compression_level ?? ''}`}
        />
      </div>

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
              const pct = totalDisk > 0 ? (b.size / totalDisk) * 100 : 0
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
