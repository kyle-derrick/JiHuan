import { useEffect, useState } from 'react'
import { Settings2, RefreshCw, Eye, EyeOff, Lock } from 'lucide-react'
import { getStatus, getStoredApiKey, setStoredApiKey, getConfig, changePassword } from '@/api'
import { formatBytes } from '@/lib/utils'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyObj = Record<string, any>

function ConfigSection({ title, data, hints = {} }: { title: string; data: AnyObj; hints?: Record<string, string> }) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5">
      <h3 className="text-sm font-semibold text-gray-800 mb-3">{title}</h3>
      <dl className="text-sm">
        {Object.entries(data).map(([k, v]) => {
          let display: string
          if (v === null || v === undefined) display = '—'
          else if (typeof v === 'object') display = JSON.stringify(v)
          else if (typeof v === 'number' && /size|bytes/i.test(k)) display = `${formatBytes(v)} (${v})`
          else display = String(v)
          return (
            <div key={k} className="flex justify-between items-start py-2 border-b border-gray-100 last:border-0">
              <div className="flex-shrink-0">
                <dt className="text-gray-700 font-mono text-xs">{k}</dt>
                {hints[k] && <div className="text-xs text-gray-400 mt-0.5">{hints[k]}</div>}
              </div>
              <dd className="font-mono text-xs text-gray-800 break-all max-w-xs text-right">{display}</dd>
            </div>
          )
        })}
      </dl>
    </div>
  )
}

export default function Settings() {
  const [version, setVersion] = useState('—')
  const [apiKey, setApiKey] = useState(getStoredApiKey)
  const [showKey, setShowKey] = useState(false)
  const [saved, setSaved] = useState(false)
  const [config, setConfig] = useState<AnyObj | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  // Password change state — rotates the authenticated caller's own credential.
  const [pw1, setPw1] = useState('')
  const [pw2, setPw2] = useState('')
  const [pwBusy, setPwBusy] = useState(false)
  const [pwMsg, setPwMsg] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null)

  const submitPassword = async () => {
    setPwMsg(null)
    if (pw1.length < 8) {
      setPwMsg({ kind: 'err', text: '密码至少 8 个字符' })
      return
    }
    if (pw1 !== pw2) {
      setPwMsg({ kind: 'err', text: '两次输入不一致' })
      return
    }
    setPwBusy(true)
    try {
      await changePassword(pw1)
      setPw1('')
      setPw2('')
      setPwMsg({ kind: 'ok', text: '密码已更新。下次登录使用新密码（当前会话继续有效）。' })
    } catch (e) {
      setPwMsg({ kind: 'err', text: e instanceof Error ? e.message : String(e) })
    } finally {
      setPwBusy(false)
    }
  }

  const loadAll = async () => {
    setLoading(true)
    setError('')
    try {
      const [s, c] = await Promise.all([getStatus().catch(() => null), getConfig()])
      if (s) setVersion(s.version)
      setConfig(c)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadAll() }, [])

  const save = () => {
    setStoredApiKey(apiKey)
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  const storageHints: Record<string, string> = {
    data_dir: '块文件存储目录',
    meta_dir: '元数据数据库目录',
    wal_dir: 'WAL 日志目录',
    block_file_size: '单个 Block 文件最大字节数',
    chunk_size: '每个 Chunk 的字节数（去重粒度）',
    hash_algorithm: '内容哈希算法（去重依据）',
    compression_algorithm: '压缩算法',
    compression_level: '压缩级别',
    time_partition_hours: '按小时分区的时长',
    gc_threshold: 'GC 触发阈值（0-1）',
    gc_interval_secs: '后台 GC 间隔（秒）',
    max_open_block_files: 'LRU 缓存最多保留的 Block 文件句柄数',
    verify_on_read: '读取时校验哈希',
  }
  const serverHints: Record<string, string> = {
    http_addr: 'HTTP 监听地址',
    grpc_addr: 'gRPC 监听地址',
    metrics_addr: 'Prometheus 监听地址',
    worker_threads: 'Tokio worker 线程数',
    max_body_size: '上传请求体最大字节数（null = 不限）',
    enable_access_log: '是否记录访问日志',
  }
  const authHints: Record<string, string> = {
    enabled: '是否启用 API Key 认证',
    exempt_routes: '免认证的路由前缀列表',
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Settings2 className="text-gray-500" size={22} />
          <h1 className="text-2xl font-bold text-gray-900">设置</h1>
        </div>
        <button
          onClick={loadAll}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-2 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          刷新
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200">{error}</div>
      )}

      {/* API Key card */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 mb-5 max-w-2xl">
        <h2 className="text-sm font-semibold text-gray-800 mb-3">API Key（浏览器本地）</h2>
        <p className="text-xs text-gray-500 mb-3">
          启用认证后，所有 API 请求需携带密钥（保存到浏览器 localStorage）。
        </p>
        <div className="flex gap-2">
          <div className="relative flex-1">
            <input
              type={showKey ? 'text' : 'password'}
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder="jh_xxxx…"
              className="w-full px-3 py-2 pr-10 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300 font-mono"
            />
            <button
              onClick={() => setShowKey(!showKey)}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
              type="button"
            >
              {showKey ? <EyeOff size={15} /> : <Eye size={15} />}
            </button>
          </div>
          <button
            onClick={save}
            className="px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700"
          >
            {saved ? '已保存 ✓' : '保存'}
          </button>
        </div>
      </div>

      {/* Change password */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 mb-5 max-w-2xl">
        <div className="flex items-center gap-2 mb-3">
          <Lock size={14} className="text-gray-500" />
          <h2 className="text-sm font-semibold text-gray-800">修改登录密码</h2>
        </div>
        <p className="text-xs text-gray-500 mb-3">
          轮换当前登录账户的凭证。保存后，启动时打印的 bootstrap key（或旧密码）立即失效；
          当前浏览器会话保留，下次登录需使用新密码。
        </p>
        <div className="space-y-2.5">
          <input
            type="password"
            value={pw1}
            onChange={(e) => setPw1(e.target.value)}
            placeholder="新密码（≥8 字符）"
            autoComplete="new-password"
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300 font-mono"
          />
          <input
            type="password"
            value={pw2}
            onChange={(e) => setPw2(e.target.value)}
            placeholder="再次输入新密码"
            autoComplete="new-password"
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300 font-mono"
          />
          {pwMsg && (
            <div
              className={
                pwMsg.kind === 'ok'
                  ? 'text-xs px-3 py-2 rounded-md bg-emerald-50 text-emerald-700 border border-emerald-200'
                  : 'text-xs px-3 py-2 rounded-md bg-red-50 text-red-700 border border-red-200 break-all'
              }
            >
              {pwMsg.text}
            </div>
          )}
          <button
            onClick={submitPassword}
            disabled={pwBusy}
            className="px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
          >
            {pwBusy ? '保存中…' : '保存新密码'}
          </button>
        </div>
      </div>

      {/* About */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 mb-5 max-w-2xl">
        <h2 className="text-sm font-semibold text-gray-800 mb-3">关于</h2>
        <dl className="space-y-2 text-sm">
          <div className="flex justify-between py-1.5 border-b border-gray-100">
            <dt className="text-gray-500">服务版本</dt>
            <dd className="font-mono text-gray-800">{version}</dd>
          </div>
          <div className="flex justify-between py-1.5">
            <dt className="text-gray-500">UI 地址</dt>
            <dd className="font-mono text-gray-800">{window.location.origin}</dd>
          </div>
        </dl>
      </div>

      {/* Live config */}
      <h2 className="text-lg font-semibold text-gray-800 mb-3 mt-8">当前配置（只读）</h2>
      {loading && !config ? (
        <div className="text-gray-400 text-sm">加载中…</div>
      ) : config ? (
        <div className="grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
          {config.storage && <ConfigSection title="Storage" data={config.storage as AnyObj} hints={storageHints} />}
          {config.server && <ConfigSection title="Server" data={config.server as AnyObj} hints={serverHints} />}
          {config.auth && <ConfigSection title="Auth" data={config.auth as AnyObj} hints={authHints} />}
        </div>
      ) : null}
    </div>
  )
}
