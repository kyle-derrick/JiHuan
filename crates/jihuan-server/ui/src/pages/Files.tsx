import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Upload, Download, Trash2, Eye, RefreshCw, Search,
  ArrowUp, ArrowDown, CheckSquare, Square, X,
} from 'lucide-react'
import {
  uploadFile, deleteFile, downloadFile, getFileMeta, listFiles,
  type FileMeta, type ListFilesParams,
} from '@/api'
import { formatBytes, formatUnixTime } from '@/lib/utils'

type SortKey = 'create_time' | 'file_name' | 'file_size'
type SortOrder = 'asc' | 'desc'

export default function Files() {
  const [files, setFiles] = useState<FileMeta[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [query, setQuery] = useState('')
  const [appliedQuery, setAppliedQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('create_time')
  const [order, setOrder] = useState<SortOrder>('desc')
  const [page, setPage] = useState(0)
  const [pageSize] = useState(50)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [uploading, setUploading] = useState(false)
  const [uploadPct, setUploadPct] = useState(0)
  const [uploadName, setUploadName] = useState('')
  const [detail, setDetail] = useState<FileMeta | null>(null)
  const [autoRefresh, setAutoRefresh] = useState<number>(0) // seconds, 0 = off
  const fileRef = useRef<HTMLInputElement>(null)

  const loadFiles = async () => {
    setLoading(true)
    setError('')
    try {
      const params: ListFilesParams = {
        limit: pageSize,
        offset: page * pageSize,
        q: appliedQuery || undefined,
        sort,
        order,
      }
      const r = await listFiles(params)
      setFiles(r.files)
      setTotal(r.total)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadFiles() }, [page, sort, order, appliedQuery])

  useEffect(() => {
    if (!autoRefresh) return
    const id = setInterval(() => { loadFiles() }, autoRefresh * 1000)
    return () => clearInterval(id)
  }, [autoRefresh, page, sort, order, appliedQuery])

  const handleUpload = async (f: File) => {
    setUploading(true)
    setUploadPct(0)
    setUploadName(f.name)
    setError('')
    try {
      await uploadFile(f, setUploadPct)
      setPage(0)
      await loadFiles()
    } catch (e) {
      setError(String(e))
    } finally {
      setUploading(false)
      setUploadName('')
    }
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    const f = e.dataTransfer.files[0]
    if (f) handleUpload(f)
  }

  const handleDelete = async (fileId: string) => {
    if (!confirm('确认删除？')) return
    try {
      await deleteFile(fileId)
      setSelected(prev => {
        const n = new Set(prev); n.delete(fileId); return n
      })
      await loadFiles()
    } catch (e) {
      setError(String(e))
    }
  }

  const handleBatchDelete = async () => {
    if (selected.size === 0) return
    if (!confirm(`确认删除选中的 ${selected.size} 个文件？`)) return
    setLoading(true)
    setError('')
    try {
      const results = await Promise.allSettled(
        Array.from(selected).map(id => deleteFile(id))
      )
      const failed = results.filter(r => r.status === 'rejected').length
      if (failed > 0) setError(`${failed}/${selected.size} 个删除失败`)
      setSelected(new Set())
      await loadFiles()
    } finally {
      setLoading(false)
    }
  }

  const showDetail = async (f: FileMeta) => {
    try {
      const full = await getFileMeta(f.file_id)
      setDetail(full)
    } catch (e) {
      setError(String(e))
    }
  }

  const toggleSelect = (id: string) => {
    setSelected(prev => {
      const n = new Set(prev)
      if (n.has(id)) n.delete(id); else n.add(id)
      return n
    })
  }

  const toggleSelectAll = () => {
    if (selected.size === files.length) setSelected(new Set())
    else setSelected(new Set(files.map(f => f.file_id)))
  }

  const onSort = (key: SortKey) => {
    if (key === sort) {
      setOrder(order === 'desc' ? 'asc' : 'desc')
    } else {
      setSort(key)
      setOrder('desc')
    }
    setPage(0)
  }

  const applySearch = () => {
    setAppliedQuery(query.trim())
    setPage(0)
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  const SortHeader = ({ label, k }: { label: string; k: SortKey }) => (
    <button
      onClick={() => onSort(k)}
      className="flex items-center gap-1 font-medium text-gray-500 hover:text-indigo-600"
    >
      {label}
      {sort === k && (order === 'desc' ? <ArrowDown size={12} /> : <ArrowUp size={12} />)}
    </button>
  )

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">文件管理</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            {loading ? '加载中…' : `共 ${total} 个文件${appliedQuery ? `（已过滤: "${appliedQuery}"）` : ''}`}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={autoRefresh}
            onChange={e => setAutoRefresh(Number(e.target.value))}
            className="px-2 py-2 text-sm border border-gray-200 rounded-lg"
          >
            <option value={0}>不自动刷新</option>
            <option value={5}>每 5s 刷新</option>
            <option value={10}>每 10s 刷新</option>
            <option value={30}>每 30s 刷新</option>
          </select>
          <button
            onClick={loadFiles}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            刷新
          </button>
          <button
            onClick={() => fileRef.current?.click()}
            disabled={uploading}
            className="flex items-center gap-1.5 px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
          >
            <Upload size={15} />
            上传文件
          </button>
        </div>
        <input ref={fileRef} type="file" className="hidden" onChange={(e) => e.target.files?.[0] && handleUpload(e.target.files[0])} />
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200 flex justify-between">
          <span>{error}</span>
          <button onClick={() => setError('')}><X size={14} /></button>
        </div>
      )}

      {/* Drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        className="mb-5 border-2 border-dashed border-gray-200 rounded-xl p-6 text-center text-gray-400 hover:border-indigo-300 transition-colors"
      >
        {uploading ? (
          <div>
            <div className="text-sm mb-2 text-gray-600">
              上传中 <span className="font-mono">{uploadName}</span> — {uploadPct}%
            </div>
            <div className="h-2 bg-gray-100 rounded-full overflow-hidden w-64 mx-auto">
              <div className="h-full bg-indigo-500 transition-all" style={{ width: `${uploadPct}%` }} />
            </div>
          </div>
        ) : (
          <span className="text-sm">将文件拖拽到此处，或点击上方按钮上传</span>
        )}
      </div>

      {/* Search + batch */}
      <div className="mb-4 flex gap-2 items-center">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={15} />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applySearch()}
            placeholder="按文件名或 File ID 搜索（服务端）…"
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300"
          />
        </div>
        <button
          onClick={applySearch}
          className="px-4 py-2 text-sm bg-white border border-gray-200 rounded-lg hover:bg-gray-50"
        >
          搜索
        </button>
        {appliedQuery && (
          <button
            onClick={() => { setQuery(''); setAppliedQuery(''); setPage(0) }}
            className="px-3 py-2 text-sm text-gray-500 hover:text-gray-700"
          >
            清除
          </button>
        )}
        {selected.size > 0 && (
          <button
            onClick={handleBatchDelete}
            className="flex items-center gap-1.5 px-4 py-2 text-sm bg-red-50 text-red-700 border border-red-200 rounded-lg hover:bg-red-100"
          >
            <Trash2 size={14} />
            删除选中 ({selected.size})
          </button>
        )}
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left">
              <th className="px-4 py-3 w-10">
                <button onClick={toggleSelectAll} className="text-gray-400 hover:text-indigo-600">
                  {selected.size > 0 && selected.size === files.length
                    ? <CheckSquare size={15} />
                    : <Square size={15} />}
                </button>
              </th>
              <th className="px-4 py-3"><SortHeader label="文件名" k="file_name" /></th>
              <th className="px-4 py-3"><SortHeader label="大小" k="file_size" /></th>
              <th className="px-4 py-3 font-medium text-gray-500">类型</th>
              <th className="px-4 py-3"><SortHeader label="创建时间" k="create_time" /></th>
              <th className="px-4 py-3 font-medium text-gray-500 w-32">操作</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={6} className="px-4 py-10 text-center text-gray-400">加载中…</td></tr>
            ) : files.length === 0 ? (
              <tr><td colSpan={6} className="px-4 py-10 text-center text-gray-400">
                {appliedQuery ? '没有匹配的文件' : '暂无文件，请上传'}
              </td></tr>
            ) : (
              files.map((f) => (
                <tr key={f.file_id} className={`border-b border-gray-50 hover:bg-gray-50 ${selected.has(f.file_id) ? 'bg-indigo-50/40' : ''}`}>
                  <td className="px-4 py-3">
                    <button onClick={() => toggleSelect(f.file_id)} className="text-gray-400 hover:text-indigo-600">
                      {selected.has(f.file_id) ? <CheckSquare size={15} /> : <Square size={15} />}
                    </button>
                  </td>
                  <td className="px-4 py-3">
                    <div className="font-medium text-gray-900">{f.file_name}</div>
                    <div className="text-xs text-gray-400 font-mono">{f.file_id}</div>
                  </td>
                  <td className="px-4 py-3 text-gray-600">{formatBytes(f.file_size)}</td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{f.content_type ?? '—'}</td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{formatUnixTime(f.create_time)}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1">
                      <button onClick={() => showDetail(f)} className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded" title="详情">
                        <Eye size={14} />
                      </button>
                      <button onClick={() => downloadFile(f.file_id, f.file_name)} className="p-1.5 text-gray-400 hover:text-green-600 hover:bg-green-50 rounded" title="下载">
                        <Download size={14} />
                      </button>
                      <button onClick={() => handleDelete(f.file_id)} className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded" title="删除">
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>

        {/* Pagination */}
        {total > pageSize && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>第 {page + 1} / {totalPages} 页（共 {total} 条）</span>
            <div className="flex gap-1">
              <button
                onClick={() => setPage(p => Math.max(0, p - 1))}
                disabled={page === 0}
                className="px-3 py-1 border border-gray-200 rounded disabled:opacity-40 hover:bg-gray-50"
              >上一页</button>
              <button
                onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                disabled={page >= totalPages - 1}
                className="px-3 py-1 border border-gray-200 rounded disabled:opacity-40 hover:bg-gray-50"
              >下一页</button>
            </div>
          </div>
        )}
      </div>

      {/* Detail modal */}
      {detail && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4" onClick={() => setDetail(null)}>
          <div className="bg-white rounded-2xl p-6 w-full max-w-3xl max-h-[85vh] overflow-auto shadow-xl" onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold">文件详情</h2>
              <button onClick={() => setDetail(null)} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
            </div>
            <dl className="space-y-1 text-sm mb-5">
              {[
                ['File ID', detail.file_id],
                ['文件名', detail.file_name],
                ['大小', formatBytes(detail.file_size)],
                ['类型', detail.content_type ?? '—'],
                ['Chunk 数', String(detail.chunk_count)],
                ['时间分区', detail.partition_id != null ? String(detail.partition_id) : '—'],
                ['创建时间', formatUnixTime(detail.create_time)],
              ].map(([k, v]) => (
                <div key={k} className="flex justify-between py-1.5 border-b border-gray-100">
                  <dt className="text-gray-500">{k}</dt>
                  <dd className="font-mono text-gray-800 break-all max-w-xl text-right">{v}</dd>
                </div>
              ))}
            </dl>

            {detail.chunks && detail.chunks.length > 0 && (
              <>
                <h3 className="text-sm font-semibold text-gray-700 mb-2">Chunks ({detail.chunks.length})</h3>
                <div className="overflow-auto border border-gray-100 rounded-lg">
                  <table className="w-full text-xs">
                    <thead className="bg-gray-50">
                      <tr className="text-left text-gray-500">
                        <th className="px-3 py-2">#</th>
                        <th className="px-3 py-2">Block ID</th>
                        <th className="px-3 py-2">Offset</th>
                        <th className="px-3 py-2">原始</th>
                        <th className="px-3 py-2">压缩</th>
                        <th className="px-3 py-2">Hash</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.chunks.map((c) => (
                        <tr key={`${c.index}-${c.block_id}-${c.offset}`} className="border-t border-gray-100">
                          <td className="px-3 py-1.5 font-mono text-gray-600">{c.index}</td>
                          <td className="px-3 py-1.5 font-mono">
                            <Link to={`/ui/blocks?block=${c.block_id}`} className="text-indigo-600 hover:underline">
                              {c.block_id.slice(0, 12)}…
                            </Link>
                          </td>
                          <td className="px-3 py-1.5 font-mono text-gray-600">{c.offset}</td>
                          <td className="px-3 py-1.5 text-gray-600">{formatBytes(c.original_size)}</td>
                          <td className="px-3 py-1.5 text-gray-600">
                            {formatBytes(c.compressed_size)}
                            {c.original_size > 0 && (
                              <span className="text-gray-400 ml-1">
                                ({(c.compressed_size / c.original_size * 100).toFixed(0)}%)
                              </span>
                            )}
                          </td>
                          <td className="px-3 py-1.5 font-mono text-gray-500">
                            {c.hash ? c.hash.slice(0, 10) + '…' : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            <button
              onClick={() => setDetail(null)}
              className="mt-5 w-full py-2 text-sm bg-gray-100 rounded-lg hover:bg-gray-200"
            >关闭</button>
          </div>
        </div>
      )}
    </div>
  )
}
