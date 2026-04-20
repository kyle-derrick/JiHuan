import { useEffect, useState } from 'react'
import { Settings2 } from 'lucide-react'
import { getStatus, getStoredApiKey, setStoredApiKey } from '@/api'

export default function Settings() {
  const [version, setVersion] = useState('—')
  const [apiKey, setApiKey] = useState(getStoredApiKey)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    getStatus().then((s) => setVersion(s.version)).catch(() => {})
  }, [])

  const save = () => {
    setStoredApiKey(apiKey)
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  return (
    <div>
      <div className="flex items-center gap-3 mb-6">
        <Settings2 className="text-gray-500" size={22} />
        <h1 className="text-2xl font-bold text-gray-900">设置</h1>
      </div>

      <div className="space-y-5 max-w-lg">
        <div className="bg-white rounded-xl border border-gray-200 p-5">
          <h2 className="text-sm font-semibold text-gray-700 mb-3">API Key</h2>
          <p className="text-xs text-gray-400 mb-3">
            当服务器启用认证时，所有 API 请求需携带密钥。在此设置后，密钥会保存到浏览器 localStorage。
          </p>
          <input
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="jh_xxxx…"
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-300 font-mono"
          />
          <button
            onClick={save}
            className="mt-3 px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700"
          >
            {saved ? '已保存 ✓' : '保存'}
          </button>
        </div>

        <div className="bg-white rounded-xl border border-gray-200 p-5">
          <h2 className="text-sm font-semibold text-gray-700 mb-3">关于</h2>
          <dl className="space-y-2 text-sm">
            <div className="flex justify-between py-1.5 border-b border-gray-100">
              <dt className="text-gray-500">服务版本</dt>
              <dd className="font-mono text-gray-800">{version}</dd>
            </div>
            <div className="flex justify-between py-1.5">
              <dt className="text-gray-500">服务地址</dt>
              <dd className="font-mono text-gray-800">{window.location.origin}</dd>
            </div>
          </dl>
        </div>
      </div>
    </div>
  )
}
