import { useEffect, useRef, useState } from 'react'
import { Upload, Download, Trash2, Eye, RefreshCw, Search } from 'lucide-react'
import {
  uploadFile, deleteFile, downloadFile, getFileMeta,
  type FileMeta,
} from '@/api'
import { formatBytes, formatUnixTime } from '@/lib/utils'

export default function Files() {
  const [files, setFiles] = useState<FileMeta[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [search, setSearch] = useState('')
  const [uploading, setUploading] = useState(false)
  const [uploadPct, setUploadPct] = useState(0)
  const [detail, setDetail] = useState<FileMeta | null>(null)
  const [lookupId, setLookupId] = useState('')
  const fileRef = useRef<HTMLInputElement>(null)

  const fetchFile = async (id: string) => {
    try {
      const m = await getFileMeta(id)
      setFiles((prev) => {
        const exists = prev.find((f) => f.file_id === id)
        return exists ? prev.map((f) => (f.file_id === id ? m : f)) : [m, ...prev]
      })
    } catch (e) {
      setError(String(e))
    }
  }

  const handleUpload = async (f: File) => {
    setUploading(true)
    setUploadPct(0)
    setError('')
    try {
      const r = await uploadFile(f, setUploadPct)
      await fetchFile(r.file_id)
    } catch (e) {
      setError(String(e))
    } finally {
      setUploading(false)
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
      setFiles((prev) => prev.filter((f) => f.file_id !== fileId))
    } catch (e) {
      setError(String(e))
    }
  }

  const handleLookup = async () => {
    if (!lookupId.trim()) return
    setLoading(true)
    setError('')
    try {
      await fetchFile(lookupId.trim())
      setLookupId('')
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  const filtered = files.filter(
    (f) =>
      f.file_name.toLowerCase().includes(search.toLowerCase()) ||
      f.file_id.includes(search)
  )

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">文件管理</h1>
        <button
          onClick={() => fileRef.current?.click()}
          disabled={uploading}
          className="flex items-center gap-1.5 px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
        >
          <Upload size={15} />
          上传文件
        </button>
        <input ref={fileRef} type="file" className="hidden" onChange={(e) => e.target.files?.[0] && handleUpload(e.target.files[0])} />
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200">{error}</div>
      )}

      {/* Drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        className="mb-5 border-2 border-dashed border-gray-200 rounded-xl p-6 text-center text-gray-400 hover:border-indigo-300 transition-colors"
      >
        {uploading ? (
          <div>
            <div className="text-sm mb-2">上传中… {uploadPct}%</div>
            <div className="h-2 bg-gray-100 rounded-full overflow-hidden w-48 mx-auto">
              <div className="h-full bg-indigo-500 transition-all" style={{ width: `${uploadPct}%` }} />
            </div>
          </div>
        ) : (
          <span className="text-sm">将文件拖拽到此处，或点击上方按钮上传</span>
        )}
      </div>

      {/* Lookup by ID */}
      <div className="mb-4 flex gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={15} />
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="在已加载文件中搜索…"
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300"
          />
        </div>
        <input
          value={lookupId}
          onChange={(e) => setLookupId(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleLookup()}
          placeholder="按 File ID 查询…"
          className="w-64 px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300"
        />
        <button
          onClick={handleLookup}
          disabled={loading}
          className="px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
        </button>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left">
              <th className="px-4 py-3 font-medium text-gray-500">文件名</th>
              <th className="px-4 py-3 font-medium text-gray-500">大小</th>
              <th className="px-4 py-3 font-medium text-gray-500">类型</th>
              <th className="px-4 py-3 font-medium text-gray-500">创建时间</th>
              <th className="px-4 py-3 font-medium text-gray-500 w-32">操作</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-10 text-center text-gray-400">
                  暂无文件，请上传或按 ID 查询
                </td>
              </tr>
            ) : (
              filtered.map((f) => (
                <tr key={f.file_id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-4 py-3">
                    <div className="font-medium text-gray-900">{f.file_name}</div>
                    <div className="text-xs text-gray-400 font-mono">{f.file_id}</div>
                  </td>
                  <td className="px-4 py-3 text-gray-600">{formatBytes(f.file_size)}</td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{f.content_type ?? '—'}</td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{formatUnixTime(f.create_time)}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => setDetail(f)}
                        className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded"
                        title="详情"
                      >
                        <Eye size={14} />
                      </button>
                      <button
                        onClick={() => downloadFile(f.file_id, f.file_name)}
                        className="p-1.5 text-gray-400 hover:text-green-600 hover:bg-green-50 rounded"
                        title="下载"
                      >
                        <Download size={14} />
                      </button>
                      <button
                        onClick={() => handleDelete(f.file_id)}
                        className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded"
                        title="删除"
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Detail modal */}
      {detail && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50" onClick={() => setDetail(null)}>
          <div className="bg-white rounded-2xl p-6 w-full max-w-md shadow-xl" onClick={(e) => e.stopPropagation()}>
            <h2 className="text-lg font-semibold mb-4">文件详情</h2>
            <dl className="space-y-2 text-sm">
              {[
                ['File ID', detail.file_id],
                ['文件名', detail.file_name],
                ['大小', formatBytes(detail.file_size)],
                ['类型', detail.content_type ?? '—'],
                ['Chunk 数', String(detail.chunk_count)],
                ['创建时间', formatUnixTime(detail.create_time)],
              ].map(([k, v]) => (
                <div key={k} className="flex justify-between py-1.5 border-b border-gray-100 last:border-0">
                  <dt className="text-gray-500">{k}</dt>
                  <dd className="font-mono text-gray-800 break-all max-w-xs text-right">{v}</dd>
                </div>
              ))}
            </dl>
            <button
              onClick={() => setDetail(null)}
              className="mt-5 w-full py-2 text-sm bg-gray-100 rounded-lg hover:bg-gray-200"
            >
              关闭
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
