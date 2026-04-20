import { useEffect, useState } from 'react'
import { Plus, Trash2, RefreshCw, Copy, Check, KeyRound } from 'lucide-react'
import { listKeys, createKey, deleteKey, type KeyInfo, type CreateKeyResponse } from '@/api'
import { formatUnixTime, timeAgo } from '@/lib/utils'

export default function Keys() {
  const [keys, setKeys] = useState<KeyInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newKey, setNewKey] = useState<CreateKeyResponse | null>(null)
  const [copied, setCopied] = useState(false)

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const r = await listKeys()
      setKeys(r.keys)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleCreate = async () => {
    if (!newName.trim()) return
    setCreating(true)
    setError('')
    try {
      const r = await createKey(newName.trim())
      setNewKey(r)
      setNewName('')
      await load()
    } catch (e) {
      setError(String(e))
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async (keyId: string) => {
    if (!confirm('确认撤销该密钥？此操作不可恢复。')) return
    try {
      await deleteKey(keyId)
      setKeys((prev) => prev.filter((k) => k.key_id !== keyId))
    } catch (e) {
      setError(String(e))
    }
  }

  const copy = (text: string) => {
    navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <KeyRound className="text-amber-500" size={22} />
          <h1 className="text-2xl font-bold text-gray-900">密钥管理</h1>
        </div>
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

      {/* New key banner */}
      {newKey && (
        <div className="mb-5 p-4 bg-amber-50 border border-amber-200 rounded-xl">
          <div className="flex items-start justify-between">
            <div>
              <div className="font-semibold text-amber-800 mb-1">密钥已创建！请立即保存，此后不会再次显示。</div>
              <div className="font-mono text-sm text-amber-900 bg-amber-100 rounded px-3 py-1.5 inline-block break-all">
                {newKey.key}
              </div>
            </div>
            <button
              onClick={() => copy(newKey.key)}
              className="ml-3 p-2 text-amber-700 hover:bg-amber-100 rounded-lg flex-shrink-0"
            >
              {copied ? <Check size={16} /> : <Copy size={16} />}
            </button>
          </div>
          <button onClick={() => setNewKey(null)} className="mt-3 text-xs text-amber-600 underline">
            我已保存，关闭提示
          </button>
        </div>
      )}

      {/* Create form */}
      <div className="mb-5 bg-white rounded-xl border border-gray-200 p-4">
        <h2 className="text-sm font-semibold text-gray-700 mb-3">创建新密钥</h2>
        <div className="flex gap-2">
          <input
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
            placeholder="密钥名称（如：production-app）"
            className="flex-1 px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300"
          />
          <button
            onClick={handleCreate}
            disabled={creating || !newName.trim()}
            className="flex items-center gap-1.5 px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
          >
            <Plus size={14} />
            创建
          </button>
        </div>
      </div>

      {/* Keys table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left">
              <th className="px-4 py-3 font-medium text-gray-500">名称</th>
              <th className="px-4 py-3 font-medium text-gray-500">前缀</th>
              <th className="px-4 py-3 font-medium text-gray-500">创建时间</th>
              <th className="px-4 py-3 font-medium text-gray-500">最后使用</th>
              <th className="px-4 py-3 font-medium text-gray-500">状态</th>
              <th className="px-4 py-3 font-medium text-gray-500 w-16">操作</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={6} className="px-4 py-10 text-center text-gray-400">加载中…</td>
              </tr>
            )}
            {!loading && keys.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-10 text-center text-gray-400">暂无密钥</td>
              </tr>
            )}
            {keys.map((k) => (
              <tr key={k.key_id} className="border-b border-gray-50 hover:bg-gray-50">
                <td className="px-4 py-3">
                  <div className="font-medium text-gray-900">{k.name}</div>
                  <div className="text-xs text-gray-400 font-mono">{k.key_id.slice(0, 16)}…</div>
                </td>
                <td className="px-4 py-3 font-mono text-xs text-gray-600">{k.key_prefix}</td>
                <td className="px-4 py-3 text-xs text-gray-500">{formatUnixTime(k.created_at)}</td>
                <td className="px-4 py-3 text-xs text-gray-500">{timeAgo(k.last_used_at)}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    k.enabled ? 'bg-green-50 text-green-700' : 'bg-gray-100 text-gray-500'
                  }`}>
                    {k.enabled ? '启用' : '禁用'}
                  </span>
                </td>
                <td className="px-4 py-3">
                  <button
                    onClick={() => handleDelete(k.key_id)}
                    className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded"
                    title="撤销"
                  >
                    <Trash2 size={14} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
