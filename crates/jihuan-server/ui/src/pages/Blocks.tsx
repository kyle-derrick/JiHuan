import { useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { listBlocks, type BlockInfo } from '@/api'
import { formatBytes, formatUnixTime } from '@/lib/utils'

export default function Blocks() {
  const [blocks, setBlocks] = useState<BlockInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const r = await listBlocks()
      setBlocks(r.blocks)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const totalSize = blocks.reduce((acc, b) => acc + b.size, 0)

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Block 管理</h1>
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
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200">{error}</div>
      )}

      <div className="mb-4 flex gap-4 text-sm text-gray-500">
        <span>共 <strong className="text-gray-900">{blocks.length}</strong> 个 Block</span>
        <span>总大小 <strong className="text-gray-900">{formatBytes(totalSize)}</strong></span>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left">
              <th className="px-4 py-3 font-medium text-gray-500">Block ID</th>
              <th className="px-4 py-3 font-medium text-gray-500">大小</th>
              <th className="px-4 py-3 font-medium text-gray-500">引用数</th>
              <th className="px-4 py-3 font-medium text-gray-500">创建时间</th>
              <th className="px-4 py-3 font-medium text-gray-500">路径</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={5} className="px-4 py-10 text-center text-gray-400">加载中…</td>
              </tr>
            )}
            {!loading && blocks.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-10 text-center text-gray-400">暂无 Block</td>
              </tr>
            )}
            {blocks.map((b) => (
              <tr key={b.block_id} className="border-b border-gray-50 hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs text-gray-700">{b.block_id}</td>
                <td className="px-4 py-3 text-gray-600">{formatBytes(b.size)}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    b.ref_count === 0 ? 'bg-red-50 text-red-600' : 'bg-green-50 text-green-700'
                  }`}>
                    {b.ref_count}
                  </span>
                </td>
                <td className="px-4 py-3 text-gray-500 text-xs">{formatUnixTime(b.create_time)}</td>
                <td className="px-4 py-3 text-gray-400 text-xs font-mono truncate max-w-xs" title={b.path}>{b.path}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
