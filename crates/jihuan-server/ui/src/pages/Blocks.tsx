import { useEffect, useState } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { RefreshCw, Trash2, Eye, X, Trash } from 'lucide-react'
import {
  listBlocks, getBlockDetail, deleteBlock, triggerGc,
  type BlockInfo, type BlockDetail,
} from '@/api'
import { formatBytes, formatUnixTime } from '@/lib/utils'

export default function Blocks() {
  const [blocks, setBlocks] = useState<BlockInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [info, setInfo] = useState('')
  const [detail, setDetail] = useState<BlockDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [gcing, setGcing] = useState(false)
  const [searchParams, setSearchParams] = useSearchParams()

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

  const showDetail = async (blockId: string) => {
    setDetailLoading(true)
    try {
      const d = await getBlockDetail(blockId)
      setDetail(d)
    } catch (e) {
      setError(String(e))
    } finally {
      setDetailLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  // Handle ?block=<id> deep link from Files page
  useEffect(() => {
    const bid = searchParams.get('block')
    if (bid) {
      showDetail(bid)
      searchParams.delete('block')
      setSearchParams(searchParams, { replace: true })
    }
  }, [searchParams])

  const handleDelete = async (blockId: string) => {
    if (!confirm('确认删除该 Block？仅当 ref_count=0 时允许。')) return
    try {
      await deleteBlock(blockId)
      setInfo(`已删除 Block ${blockId.slice(0, 12)}…`)
      setDetail(null)
      await load()
    } catch (e) {
      setError(String(e))
    }
  }

  const handleGc = async () => {
    setGcing(true)
    setError('')
    try {
      const r = await triggerGc()
      setInfo(`GC 完成：删除 ${r.blocks_deleted} 个 Block，回收 ${formatBytes(r.bytes_reclaimed)}，耗时 ${r.duration_ms}ms`)
      await load()
    } catch (e) {
      setError(String(e))
    } finally {
      setGcing(false)
    }
  }

  const totalSize = blocks.reduce((acc, b) => acc + b.size, 0)
  const activeCount = blocks.filter(b => !b.sealed).length
  const orphanCount = blocks.filter(b => b.ref_count === 0).length

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Block 管理</h1>
        <div className="flex gap-2">
          <button
            onClick={handleGc}
            disabled={gcing}
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-amber-700 bg-amber-50 border border-amber-200 rounded-lg hover:bg-amber-100 disabled:opacity-50"
          >
            <Trash size={14} className={gcing ? 'animate-spin' : ''} />
            {gcing ? 'GC 运行中…' : '触发 GC'}
          </button>
          <button
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            刷新
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200 flex justify-between">
          <span>{error}</span>
          <button onClick={() => setError('')}><X size={14} /></button>
        </div>
      )}
      {info && (
        <div className="mb-4 p-3 bg-green-50 text-green-700 rounded-lg text-sm border border-green-200 flex justify-between">
          <span>{info}</span>
          <button onClick={() => setInfo('')}><X size={14} /></button>
        </div>
      )}

      <div className="mb-4 flex gap-4 text-sm text-gray-500 flex-wrap">
        <span>共 <strong className="text-gray-900">{blocks.length}</strong> 个 Block</span>
        <span>总大小 <strong className="text-gray-900">{formatBytes(totalSize)}</strong></span>
        <span>活跃（未 seal）<strong className="text-amber-700">{activeCount}</strong></span>
        <span>孤立（ref=0）<strong className="text-red-700">{orphanCount}</strong></span>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left">
              <th className="px-4 py-3 font-medium text-gray-500">Block ID</th>
              <th className="px-4 py-3 font-medium text-gray-500">状态</th>
              <th className="px-4 py-3 font-medium text-gray-500">大小</th>
              <th className="px-4 py-3 font-medium text-gray-500">引用数</th>
              <th className="px-4 py-3 font-medium text-gray-500">创建时间</th>
              <th className="px-4 py-3 font-medium text-gray-500 w-28">操作</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={6} className="px-4 py-10 text-center text-gray-400">加载中…</td></tr>
            )}
            {!loading && blocks.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-10 text-center text-gray-400">暂无 Block</td></tr>
            )}
            {blocks.map((b) => (
              <tr key={b.block_id} className="border-b border-gray-50 hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs text-gray-700">{b.block_id}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    b.sealed ? 'bg-blue-50 text-blue-700' : 'bg-amber-50 text-amber-700'
                  }`}>
                    {b.sealed ? 'sealed' : 'active'}
                  </span>
                </td>
                <td className="px-4 py-3 text-gray-600">{formatBytes(b.size)}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    b.ref_count === 0 ? 'bg-red-50 text-red-600' : 'bg-green-50 text-green-700'
                  }`}>
                    {b.ref_count}
                  </span>
                </td>
                <td className="px-4 py-3 text-gray-500 text-xs">{formatUnixTime(b.create_time)}</td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => showDetail(b.block_id)}
                      className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded"
                      title="详情"
                    >
                      <Eye size={14} />
                    </button>
                    <button
                      onClick={() => handleDelete(b.block_id)}
                      disabled={b.ref_count > 0}
                      className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-gray-400"
                      title={b.ref_count > 0 ? `有 ${b.ref_count} 个引用，无法删除` : '回收（ref_count=0）'}
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Detail modal */}
      {(detail || detailLoading) && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4" onClick={() => { setDetail(null); setDetailLoading(false) }}>
          <div className="bg-white rounded-2xl p-6 w-full max-w-3xl max-h-[85vh] overflow-auto shadow-xl" onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold">Block 详情</h2>
              <button onClick={() => { setDetail(null); setDetailLoading(false) }} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
            </div>

            {detailLoading && !detail && <div className="text-center text-gray-400 py-8">加载中…</div>}

            {detail && (
              <>
                <dl className="space-y-1 text-sm mb-5">
                  {[
                    ['Block ID', detail.block_id],
                    ['状态', detail.sealed ? 'sealed（已封存）' : 'active（写入中）'],
                    ['大小', formatBytes(detail.size)],
                    ['引用数', String(detail.ref_count)],
                    ['路径', detail.path],
                    ['创建时间', formatUnixTime(detail.create_time)],
                  ].map(([k, v]) => (
                    <div key={k} className="flex justify-between py-1.5 border-b border-gray-100">
                      <dt className="text-gray-500">{k}</dt>
                      <dd className="font-mono text-gray-800 break-all max-w-xl text-right">{v}</dd>
                    </div>
                  ))}
                </dl>

                <h3 className="text-sm font-semibold text-gray-700 mb-2">
                  引用此 Block 的文件（{detail.referencing_files.length}）
                </h3>
                {detail.referencing_files.length === 0 ? (
                  <div className="text-center text-gray-400 py-4 text-sm border border-dashed border-gray-200 rounded-lg">
                    无文件引用此 Block — 可安全删除
                  </div>
                ) : (
                  <div className="overflow-auto border border-gray-100 rounded-lg">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-50">
                        <tr className="text-left text-gray-500">
                          <th className="px-3 py-2">文件名</th>
                          <th className="px-3 py-2">文件大小</th>
                          <th className="px-3 py-2">块内 Chunks</th>
                          <th className="px-3 py-2">File ID</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.referencing_files.map((f) => (
                          <tr key={f.file_id} className="border-t border-gray-100 hover:bg-gray-50">
                            <td className="px-3 py-1.5 text-gray-900">{f.file_name}</td>
                            <td className="px-3 py-1.5 text-gray-600">{formatBytes(f.file_size)}</td>
                            <td className="px-3 py-1.5 text-gray-600">{f.chunks_in_block}</td>
                            <td className="px-3 py-1.5 font-mono text-gray-500">
                              <Link to={`/ui/files`} className="text-indigo-600 hover:underline">
                                {f.file_id.slice(0, 12)}…
                              </Link>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                <div className="mt-5 flex gap-2">
                  <button
                    onClick={() => handleDelete(detail.block_id)}
                    disabled={detail.ref_count > 0}
                    className="flex-1 py-2 text-sm bg-red-50 text-red-700 border border-red-200 rounded-lg hover:bg-red-100 disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    {detail.ref_count > 0 ? `有 ${detail.ref_count} 个引用，无法删除` : '回收此 Block'}
                  </button>
                  <button
                    onClick={() => { setDetail(null); setDetailLoading(false) }}
                    className="flex-1 py-2 text-sm bg-gray-100 rounded-lg hover:bg-gray-200"
                  >关闭</button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
