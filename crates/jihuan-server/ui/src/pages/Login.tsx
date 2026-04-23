import { useState, type FormEvent } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { HardDrive, KeyRound, Loader2, User } from 'lucide-react'
import { login } from '@/api'

/**
 * /ui/login — v0.5.0-iam: username + password form. On success the server
 * sets an HttpOnly `jh_session` cookie; we then navigate back to the
 * `?next=` path (or the dashboard by default).
 */
export default function Login() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const navigate = useNavigate()
  const [params] = useSearchParams()
  const next = params.get('next') || '/ui/'

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault()
    const u = username.trim()
    if (!u || !password) {
      setError('请填写用户名和密码')
      return
    }
    setLoading(true)
    setError('')
    try {
      await login(u, password)
      // Clear any stale `X-API-Key` localStorage value — the cookie is now the
      // source of truth for the UI session.
      localStorage.removeItem('jihuan_api_key')
      navigate(next, { replace: true })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-50 via-white to-indigo-100 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Brand */}
        <div className="flex flex-col items-center mb-8">
          <div className="bg-white rounded-2xl p-3 shadow-sm border border-gray-200 mb-3">
            <HardDrive className="text-indigo-600" size={32} />
          </div>
          <h1 className="text-2xl font-bold text-gray-900">JiHuan 存储控制台</h1>
          <p className="text-sm text-gray-500 mt-1">使用用户名和密码登录</p>
        </div>

        {/* Card */}
        <form
          onSubmit={onSubmit}
          className="bg-white rounded-2xl border border-gray-200 shadow-sm p-6 space-y-4"
        >
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              用户名
            </label>
            <div className="relative">
              <User
                size={15}
                className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400"
              />
              <input
                type="text"
                autoFocus
                autoComplete="username"
                spellCheck={false}
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="root"
                className="w-full pl-9 pr-3 py-2.5 text-sm font-mono border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-300"
              />
            </div>
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              密码
            </label>
            <div className="relative">
              <KeyRound
                size={15}
                className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400"
              />
              <input
                type="password"
                autoComplete="current-password"
                spellCheck={false}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                className="w-full pl-9 pr-3 py-2.5 text-sm font-mono border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-300"
              />
            </div>
            <p className="text-xs text-gray-400 mt-1.5">
              首次启动时请在服务端终端查看 root 账户的初始密码。
            </p>
          </div>

          {error && (
            <div className="p-3 bg-red-50 text-red-700 rounded-lg text-xs border border-red-200 break-all">
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? (
              <>
                <Loader2 size={15} className="animate-spin" />
                登录中…
              </>
            ) : (
              '登录'
            )}
          </button>
        </form>

        <p className="text-center text-xs text-gray-400 mt-6">
          服务端 <code className="font-mono">auth.enabled = false</code> 时无需登录。
        </p>
      </div>
    </div>
  )
}
