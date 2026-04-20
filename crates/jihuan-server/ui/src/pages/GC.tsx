import { useState } from 'react'
import { Trash2, Play } from 'lucide-react'
import { triggerGc, type GcResponse } from '@/api'
import { formatBytes } from '@/lib/utils'

export default function GC() {
  const [running, setRunning] = useState(false)
  const [history, setHistory] = useState<(GcResponse & { ts: string })[]>([])
  const [error, setError] = useState('')

  const run = async () => {
    setRunning(true)
    setError('')
    try {
      const r = await triggerGc()
      setHistory((prev) => [{ ...r, ts: new Date().toLocaleString() }, ...prev])
    } catch (e) {
      setError(String(e))
    } finally {
      setRunning(false)
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Trash2 className="text-orange-500" size={22} />
          <h1 className="text-2xl font-bold text-gray-900">垃圾回收 (GC)</h1>
        </div>
        <button
          onClick={run}
          disabled={running}
          className="flex items-center gap-2 px-4 py-2 text-sm bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50"
        >
          <Play size={14} />
          {running ? '运行中…' : '立即触发 GC'}
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200">{error}</div>
      )}

      <div className="bg-white rounded-xl border border-gray-200 p-5 mb-5">
        <h2 className="text-base font-semibold text-gray-800 mb-2">说明</h2>
        <p className="text-sm text-gray-500">
          GC 会扫描所有 Block 文件，删除引用计数为 0 的孤立 Block，释放磁盘空间。
          已删除文件的 Block 会在 GC 运行后被回收。
        </p>
      </div>

      {history.length > 0 && (
        <div>
          <h2 className="text-base font-semibold text-gray-800 mb-3">运行历史</h2>
          <div className="space-y-3">
            {history.map((r, i) => (
              <div key={i} className="bg-white rounded-xl border border-gray-200 p-4">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-sm font-medium text-gray-700">{r.ts}</span>
                  <span className="text-xs text-gray-400">{r.duration_ms} ms</span>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  {[
                    ['删除 Block', r.blocks_deleted],
                    ['回收空间', formatBytes(r.bytes_reclaimed)],
                    ['删除分区', r.partitions_deleted],
                    ['删除文件', r.files_deleted],
                  ].map(([k, v]) => (
                    <div key={k} className="text-center">
                      <div className="text-xl font-bold text-gray-900">{v}</div>
                      <div className="text-xs text-gray-400">{k}</div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {history.length === 0 && !running && (
        <div className="text-center py-16 text-gray-400 text-sm">
          点击"立即触发 GC"查看运行结果
        </div>
      )}
    </div>
  )
}
