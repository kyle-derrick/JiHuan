import { useEffect, useRef, useState } from 'react'
import { Activity, RefreshCw, X } from 'lucide-react'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { fetchMetricsText, parseMetrics, type ParsedMetrics } from '@/api'
import { formatBytes } from '@/lib/utils'

interface Sample {
  t: number // seconds since start
  puts: number
  gets: number
  deletes: number
  dedup: number
  bytes_written: number
  bytes_read: number
}

const MAX_SAMPLES = 60

export default function Metrics() {
  const [parsed, setParsed] = useState<ParsedMetrics | null>(null)
  const [samples, setSamples] = useState<Sample[]>([])
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const startRef = useRef<number>(Date.now())
  const lastFetchRef = useRef<number>(0)

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const text = await fetchMetricsText()
      const p = parseMetrics(text)
      setParsed(p)
      const now = Math.floor((Date.now() - startRef.current) / 1000)
      lastFetchRef.current = now
      setSamples((prev) => {
        const next: Sample = {
          t: now,
          puts: p.counters['jihuan_puts_total'] ?? 0,
          gets: p.counters['jihuan_gets_total'] ?? 0,
          deletes: p.counters['jihuan_deletes_total'] ?? 0,
          dedup: p.counters['jihuan_dedup_hits_total'] ?? 0,
          bytes_written: p.counters['jihuan_bytes_written_total'] ?? 0,
          bytes_read: p.counters['jihuan_bytes_read_total'] ?? 0,
        }
        const arr = [...prev, next].slice(-MAX_SAMPLES)
        return arr
      })
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  const put = parsed?.histograms['jihuan_put_duration_seconds']
  const get = parsed?.histograms['jihuan_get_duration_seconds']

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Activity className="text-gray-500" size={22} />
          <div>
            <h1 className="text-2xl font-bold text-gray-900">指标预览</h1>
            <p className="text-sm text-gray-400 mt-0.5">
              数据来自 Prometheus <code className="font-mono bg-gray-100 px-1 rounded">:9090/metrics</code>，5 秒刷新
            </p>
          </div>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-2 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          刷新
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200 flex justify-between">
          <div>
            <div className="font-medium">无法连接到 metrics 端点</div>
            <div className="text-xs mt-1">{error}</div>
            <div className="text-xs mt-1 text-red-600/70">
              请确认服务器启动时 <code>metrics_addr</code> 已启用，且浏览器可访问该端口（默认 9090）。
            </div>
          </div>
          <button onClick={() => setError('')}><X size={14} /></button>
        </div>
      )}

      {/* Counter cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {[
          { label: 'Put 总数', key: 'jihuan_puts_total' },
          { label: 'Get 总数', key: 'jihuan_gets_total' },
          { label: 'Delete 总数', key: 'jihuan_deletes_total' },
          { label: '去重命中', key: 'jihuan_dedup_hits_total' },
        ].map((m) => (
          <div key={m.key} className="bg-white rounded-xl border border-gray-200 p-5">
            <div className="text-sm text-gray-500">{m.label}</div>
            <div className="text-2xl font-bold text-gray-900 mt-1">
              {parsed?.counters[m.key]?.toLocaleString() ?? '—'}
            </div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <div className="bg-white rounded-xl border border-gray-200 p-5">
          <div className="text-sm text-gray-500 mb-1">写入总字节数</div>
          <div className="text-2xl font-bold text-gray-900">
            {parsed ? formatBytes(parsed.counters['jihuan_bytes_written_total'] ?? 0) : '—'}
          </div>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-5">
          <div className="text-sm text-gray-500 mb-1">读取总字节数</div>
          <div className="text-2xl font-bold text-gray-900">
            {parsed ? formatBytes(parsed.counters['jihuan_bytes_read_total'] ?? 0) : '—'}
          </div>
        </div>
      </div>

      {/* Put/Get duration histograms */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <LatencyCard title="Put 耗时" h={put} />
        <LatencyCard title="Get 耗时" h={get} />
      </div>

      {/* Trend chart */}
      <div className="bg-white rounded-xl border border-gray-200 p-5">
        <h2 className="text-sm font-semibold text-gray-800 mb-3">累计趋势（过去 {samples.length * 5 / 60 | 0}+ 分钟）</h2>
        {samples.length < 2 ? (
          <div className="text-center text-gray-400 py-12 text-sm">正在采集数据…</div>
        ) : (
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={samples}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="t" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v}s`} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip labelFormatter={(v) => `t=${v}s`} />
              <Line type="monotone" dataKey="puts" stroke="#6366f1" strokeWidth={2} dot={false} name="Puts" />
              <Line type="monotone" dataKey="gets" stroke="#10b981" strokeWidth={2} dot={false} name="Gets" />
              <Line type="monotone" dataKey="deletes" stroke="#ef4444" strokeWidth={2} dot={false} name="Deletes" />
              <Line type="monotone" dataKey="dedup" stroke="#f59e0b" strokeWidth={2} dot={false} name="Dedup Hits" />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

function LatencyCard({ title, h }: { title: string; h?: { count: number; sum: number; quantiles: Record<string, number> } }) {
  const avg = h && h.count > 0 ? (h.sum / h.count) * 1000 : null
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5">
      <h3 className="text-sm font-semibold text-gray-800 mb-3">{title}</h3>
      {!h || h.count === 0 ? (
        <div className="text-gray-400 text-sm">暂无样本</div>
      ) : (
        <dl className="space-y-1.5 text-sm">
          <div className="flex justify-between">
            <dt className="text-gray-500">平均</dt>
            <dd className="font-mono">{avg!.toFixed(2)} ms</dd>
          </div>
          <div className="flex justify-between">
            <dt className="text-gray-500">样本数</dt>
            <dd className="font-mono">{h.count}</dd>
          </div>
          {Object.entries(h.quantiles).sort().map(([q, v]) => (
            <div key={q} className="flex justify-between">
              <dt className="text-gray-500">p{(parseFloat(q) * 100).toFixed(0)}</dt>
              <dd className="font-mono">{(v * 1000).toFixed(2)} ms</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  )
}
