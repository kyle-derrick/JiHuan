import { useEffect, useState, type FormEvent } from 'react'
import {
  Loader2,
  Plus,
  RefreshCcw,
  ShieldCheck,
  Trash2,
  UserCog,
  UserCircle2,
} from 'lucide-react'
import {
  createUser,
  deleteUser,
  listUsers,
  resetUserPassword,
  updateUser,
  type UserInfo,
} from '@/api'

type UiMode = 'list' | 'create' | 'edit'

export default function UsersPage() {
  const [users, setUsers] = useState<UserInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [mode, setMode] = useState<UiMode>('list')
  const [editing, setEditing] = useState<UserInfo | null>(null)

  const refresh = async () => {
    setLoading(true)
    setError('')
    try {
      const r = await listUsers()
      setUsers(r.users)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void refresh()
  }, [])

  const handleDelete = async (u: UserInfo) => {
    if (u.is_root) return
    if (!confirm(`确认删除用户 ${u.username}? 该用户名下的 SA 会一起被吊销。`))
      return
    try {
      await deleteUser(u.username)
      await refresh()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <UserCog size={22} className="text-indigo-600" />
            用户管理
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            v0.5.0-iam: 管理登录账户 (User)。每个 User 可拥有多个 ServiceAccount
            (API Key),用于服务端到服务端调用。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => void refresh()}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg"
          >
            <RefreshCcw size={14} />
            刷新
          </button>
          <button
            onClick={() => setMode('create')}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg"
          >
            <Plus size={14} />
            新建用户
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200 break-all">
          {error}
        </div>
      )}

      {mode === 'create' && (
        <CreateUserCard
          onClose={() => setMode('list')}
          onCreated={async () => {
            setMode('list')
            await refresh()
          }}
        />
      )}
      {mode === 'edit' && editing && (
        <EditUserCard
          user={editing}
          onClose={() => {
            setMode('list')
            setEditing(null)
          }}
          onSaved={async () => {
            setMode('list')
            setEditing(null)
            await refresh()
          }}
        />
      )}

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-xs text-gray-600 uppercase tracking-wide">
            <tr>
              <th className="text-left px-4 py-2">用户名</th>
              <th className="text-left px-4 py-2">权限</th>
              <th className="text-left px-4 py-2">状态</th>
              <th className="text-left px-4 py-2">创建时间</th>
              <th className="text-right px-4 py-2">操作</th>
            </tr>
          </thead>
          <tbody>
            {loading && users.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-gray-400">
                  <Loader2 size={18} className="animate-spin inline" /> 加载中…
                </td>
              </tr>
            ) : users.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-gray-400">
                  还没有用户
                </td>
              </tr>
            ) : (
              users.map((u) => (
                <tr key={u.username} className="border-t border-gray-100">
                  <td className="px-4 py-2 font-mono">
                    <span className="inline-flex items-center gap-1.5">
                      {u.is_root ? (
                        <ShieldCheck size={14} className="text-amber-500" />
                      ) : (
                        <UserCircle2 size={14} className="text-gray-400" />
                      )}
                      {u.username}
                      {u.is_root && (
                        <span className="text-[10px] font-sans uppercase text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded">
                          root
                        </span>
                      )}
                    </span>
                  </td>
                  <td className="px-4 py-2">
                    <div className="flex flex-wrap gap-1">
                      {u.scopes.map((s) => (
                        <span
                          key={s}
                          className="text-xs px-1.5 py-0.5 bg-indigo-50 text-indigo-700 rounded"
                        >
                          {s}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="px-4 py-2">
                    {u.enabled ? (
                      <span className="text-xs text-emerald-600">启用</span>
                    ) : (
                      <span className="text-xs text-gray-400">已禁用</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-gray-500 text-xs">
                    {new Date(u.created_at * 1000).toLocaleString()}
                  </td>
                  <td className="px-4 py-2 text-right space-x-2">
                    <button
                      onClick={() => {
                        setEditing(u)
                        setMode('edit')
                      }}
                      className="text-xs text-indigo-600 hover:text-indigo-800"
                    >
                      编辑
                    </button>
                    <button
                      disabled={u.is_root}
                      onClick={() => void handleDelete(u)}
                      className="text-xs text-red-600 hover:text-red-800 disabled:text-gray-300 disabled:cursor-not-allowed inline-flex items-center gap-1"
                    >
                      <Trash2 size={12} />
                      删除
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Create / edit cards ─────────────────────────────────────────────────────

const ALL_SCOPES = ['admin', 'operator', 'viewer', 'read', 'write'] as const

function CreateUserCard({
  onClose,
  onCreated,
}: {
  onClose: () => void
  onCreated: () => void | Promise<void>
}) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [scopes, setScopes] = useState<string[]>(['viewer'])
  const [submitting, setSubmitting] = useState(false)
  const [err, setErr] = useState('')

  const toggleScope = (s: string) => {
    setScopes((xs) => (xs.includes(s) ? xs.filter((x) => x !== s) : [...xs, s]))
  }

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setErr('')
    setSubmitting(true)
    try {
      await createUser({ username, password, scopes })
      await onCreated()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form
      onSubmit={submit}
      className="mb-4 p-4 bg-white rounded-xl border border-indigo-200 space-y-3"
    >
      <div className="font-medium text-sm text-gray-800">创建用户</div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-xs text-gray-500 mb-1">用户名</label>
          <input
            required
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="w-full px-3 py-1.5 text-sm font-mono border border-gray-200 rounded-lg"
            placeholder="alice"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">密码</label>
          <input
            required
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full px-3 py-1.5 text-sm font-mono border border-gray-200 rounded-lg"
            placeholder="至少 8 个字符"
          />
        </div>
      </div>
      <div>
        <label className="block text-xs text-gray-500 mb-1">权限</label>
        <div className="flex flex-wrap gap-2">
          {ALL_SCOPES.map((s) => (
            <label
              key={s}
              className={`text-xs px-2 py-1 rounded cursor-pointer border ${
                scopes.includes(s)
                  ? 'bg-indigo-50 border-indigo-200 text-indigo-700'
                  : 'bg-white border-gray-200 text-gray-600'
              }`}
            >
              <input
                type="checkbox"
                className="hidden"
                checked={scopes.includes(s)}
                onChange={() => toggleScope(s)}
              />
              {s}
            </label>
          ))}
        </div>
      </div>
      {err && (
        <div className="text-xs text-red-600 bg-red-50 border border-red-200 rounded px-2 py-1">
          {err}
        </div>
      )}
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onClose}
          className="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg"
        >
          取消
        </button>
        <button
          disabled={submitting}
          className="px-3 py-1.5 text-sm bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg disabled:opacity-50 inline-flex items-center gap-1.5"
        >
          {submitting && <Loader2 size={14} className="animate-spin" />}
          创建
        </button>
      </div>
    </form>
  )
}

function EditUserCard({
  user,
  onClose,
  onSaved,
}: {
  user: UserInfo
  onClose: () => void
  onSaved: () => void | Promise<void>
}) {
  const [scopes, setScopes] = useState<string[]>(user.scopes)
  const [enabled, setEnabled] = useState(user.enabled)
  const [newPassword, setNewPassword] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [err, setErr] = useState('')

  const toggleScope = (s: string) => {
    setScopes((xs) => (xs.includes(s) ? xs.filter((x) => x !== s) : [...xs, s]))
  }

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setErr('')
    setSubmitting(true)
    try {
      await updateUser(user.username, { scopes, enabled })
      if (newPassword) {
        await resetUserPassword(user.username, newPassword)
      }
      await onSaved()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form
      onSubmit={submit}
      className="mb-4 p-4 bg-white rounded-xl border border-indigo-200 space-y-3"
    >
      <div className="font-medium text-sm text-gray-800">
        编辑用户 <span className="font-mono">{user.username}</span>
      </div>
      <div>
        <label className="block text-xs text-gray-500 mb-1">权限</label>
        <div className="flex flex-wrap gap-2">
          {ALL_SCOPES.map((s) => (
            <label
              key={s}
              className={`text-xs px-2 py-1 rounded cursor-pointer border ${
                scopes.includes(s)
                  ? 'bg-indigo-50 border-indigo-200 text-indigo-700'
                  : 'bg-white border-gray-200 text-gray-600'
              }`}
            >
              <input
                type="checkbox"
                className="hidden"
                checked={scopes.includes(s)}
                onChange={() => toggleScope(s)}
              />
              {s}
            </label>
          ))}
        </div>
      </div>
      <div className="flex items-center gap-2">
        <input
          id="enabled"
          type="checkbox"
          checked={enabled}
          disabled={user.is_root}
          onChange={(e) => setEnabled(e.target.checked)}
        />
        <label htmlFor="enabled" className="text-sm text-gray-700">
          启用
        </label>
        {user.is_root && (
          <span className="text-xs text-amber-600">root 用户不可禁用</span>
        )}
      </div>
      <div>
        <label className="block text-xs text-gray-500 mb-1">
          重置密码 (留空则不修改)
        </label>
        <input
          type="password"
          value={newPassword}
          onChange={(e) => setNewPassword(e.target.value)}
          className="w-full px-3 py-1.5 text-sm font-mono border border-gray-200 rounded-lg"
          placeholder="至少 8 个字符"
        />
      </div>
      {err && (
        <div className="text-xs text-red-600 bg-red-50 border border-red-200 rounded px-2 py-1">
          {err}
        </div>
      )}
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onClose}
          className="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg"
        >
          取消
        </button>
        <button
          disabled={submitting}
          className="px-3 py-1.5 text-sm bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg disabled:opacity-50 inline-flex items-center gap-1.5"
        >
          {submitting && <Loader2 size={14} className="animate-spin" />}
          保存
        </button>
      </div>
    </form>
  )
}
