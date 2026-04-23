import { useEffect, useState } from 'react'
import { ShieldAlert, RefreshCw, Filter } from 'lucide-react'
import { listAudit, type AuditEvent, type AuditResult } from '@/api'

/** Quick filter presets that map to backend query params. */
const ACTION_PRESETS: { label: string; value: string }[] = [
  { label: '全部', value: '' },
  { label: '登录/退出', value: 'auth.' },
  { label: '密码修改', value: 'auth.change_password' },
  { label: '密钥管理', value: 'key.' },
]

/** Canonical action → Chinese label. Falls back to the raw snake_case
 *  string when the action is unknown (e.g. after the server grows a new
 *  event kind and the UI hasn't been re-deployed yet). */
const ACTION_LABELS: Record<string, string> = {
  'auth.login': '登录成功',
  'auth.login_failed': '登录失败',
  'auth.logout': '登出',
  'auth.change_password': '修改密码',
  'key.create': '创建密钥',
  'key.delete': '删除密钥',
}

function actionLabel(action: string): string {
  return ACTION_LABELS[action] ?? action
}

function formatTs(unixSecs: number): string {
  return new Date(unixSecs * 1000).toLocaleString()
}

/** Map an `AuditResult` to a visible badge. Defensive against an
 *  unknown / missing discriminant (e.g. older servers that haven't been
 *  upgraded to the DTO-shaped response): we never return `undefined`,
 *  so `b.title` / `b.cls` accesses can't throw. */
function resultBadge(r: AuditResult | undefined): {
  text: string
  cls: string
  title?: string
} {
  const kind = r?.kind
  switch (kind) {
    case 'ok':
      return { text: 'OK', cls: 'bg-green-50 text-green-700 border-green-200' }
    case 'denied':
      return {
        text: '拒绝',
        cls: 'bg-yellow-50 text-yellow-700 border-yellow-200',
        title: (r as { kind: 'denied'; reason: string }).reason,
      }
    case 'error':
      return {
        text: '错误',
        cls: 'bg-red-50 text-red-700 border-red-200',
        title: (r as { kind: 'error'; message: string }).message,
      }
    default:
      return {
        text: '未知',
        cls: 'bg-gray-50 text-gray-600 border-gray-200',
        title: r ? JSON.stringify(r) : undefined,
      }
  }
}

export default function Audit() {
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [actionFilter, setActionFilter] = useState('')
  const [actorFilter, setActorFilter] = useState('')
  const [limit, setLimit] = useState(200)

  const refresh = async () => {
    setLoading(true)
    setError('')
    try {
      const r = await listAudit({
        action: actionFilter || undefined,
        actor: actorFilter || undefined,
        limit,
      })
      setEvents(r.events)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  // Initial load + whenever filters change.
  useEffect(() => {
    refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [actionFilter, actorFilter, limit])

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <ShieldAlert className="text-indigo-600" size={22} />
          <h1 className="text-2xl font-bold text-gray-900">审计日志</h1>
        </div>
        <button
          onClick={refresh}
          disabled={loading}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-white border border-gray-200 text-gray-700 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          刷新
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-xl border border-gray-200 p-4 mb-4">
        <div className="flex items-center gap-2 text-sm text-gray-500 mb-3">
          <Filter size={14} />
          过滤
        </div>
        <div className="flex flex-wrap gap-2 mb-3">
          {ACTION_PRESETS.map((p) => (
            <button
              key={p.label}
              onClick={() => setActionFilter(p.value)}
              className={
                'px-3 py-1.5 text-xs rounded-lg border transition-colors ' +
                (actionFilter === p.value
                  ? 'bg-indigo-50 text-indigo-700 border-indigo-200'
                  : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50')
              }
            >
              {p.label}
            </button>
          ))}
        </div>
        <div className="flex flex-wrap gap-3">
          <label className="flex items-center gap-2 text-xs text-gray-500">
            Action 前缀
            <input
              value={actionFilter}
              onChange={(e) => setActionFilter(e.target.value)}
              placeholder="auth."
              className="w-40 px-2 py-1 text-sm border border-gray-200 rounded bg-white text-gray-800"
            />
          </label>
          <label className="flex items-center gap-2 text-xs text-gray-500">
            Actor (key_id)
            <input
              value={actorFilter}
              onChange={(e) => setActorFilter(e.target.value)}
              placeholder="完整 key_id"
              className="w-52 px-2 py-1 text-sm border border-gray-200 rounded bg-white text-gray-800"
            />
          </label>
          <label className="flex items-center gap-2 text-xs text-gray-500">
            Limit
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="px-2 py-1 text-sm border border-gray-200 rounded bg-white text-gray-800"
            >
              {[50, 100, 200, 500, 1000].map((n) => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </label>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200">
          {error}
        </div>
      )}

      {/* Events table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-4 py-2 border-b border-gray-100 text-xs text-gray-500">
          {events.length} 条记录 {events.length >= limit && `（已截断至 ${limit}）`}
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                <th className="px-4 py-2">时间</th>
                <th className="px-4 py-2">Action</th>
                <th className="px-4 py-2">Actor</th>
                <th className="px-4 py-2">Target</th>
                <th className="px-4 py-2">IP</th>
                <th className="px-4 py-2">结果</th>
                <th className="px-4 py-2">HTTP</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {events.length === 0 && !loading && (
                <tr>
                  <td colSpan={7} className="text-center py-10 text-sm text-gray-400">
                    无记录
                  </td>
                </tr>
              )}
              {events.map((e, i) => {
                const b = resultBadge(e.result)
                return (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="px-4 py-2 text-gray-600 whitespace-nowrap">{formatTs(e.ts)}</td>
                    <td className="px-4 py-2 text-xs text-gray-800" title={e.action}>
                      {actionLabel(e.action)}
                    </td>
                    <td className="px-4 py-2 font-mono text-xs text-gray-600">
                      {e.actor_key_id ?? <span className="text-gray-400">—</span>}
                    </td>
                    <td className="px-4 py-2 font-mono text-xs text-gray-600">
                      {e.target ?? <span className="text-gray-400">—</span>}
                    </td>
                    <td className="px-4 py-2 font-mono text-xs text-gray-600">
                      {e.actor_ip ?? <span className="text-gray-400">—</span>}
                    </td>
                    <td className="px-4 py-2">
                      <span
                        title={b.title}
                        className={'px-2 py-0.5 text-xs rounded border ' + b.cls}
                      >
                        {b.text}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-xs text-gray-600">
                      {e.http_status ?? <span className="text-gray-400">—</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
