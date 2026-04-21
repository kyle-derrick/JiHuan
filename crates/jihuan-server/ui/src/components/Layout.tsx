import { NavLink, useNavigate } from 'react-router-dom'
import {
  LayoutDashboard,
  Files,
  Database,
  Trash2,
  KeyRound,
  Settings,
  HardDrive,
  Activity,
  LogOut,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { logout } from '@/api'

const nav = [
  { to: '/ui/', label: 'Dashboard', icon: LayoutDashboard, end: true },
  { to: '/ui/files', label: '文件管理', icon: Files },
  { to: '/ui/blocks', label: 'Block 管理', icon: Database },
  { to: '/ui/gc', label: 'GC', icon: Trash2 },
  { to: '/ui/metrics', label: '指标', icon: Activity },
  { to: '/ui/keys', label: '密钥管理', icon: KeyRound },
  { to: '/ui/settings', label: '设置', icon: Settings },
]

export default function Layout({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate()

  const handleLogout = async () => {
    try {
      await logout()
    } finally {
      // Even if the server call fails we still want to leave this page.
      navigate('/ui/login', { replace: true })
    }
  }

  return (
    <div className="flex h-screen bg-gray-50 text-gray-800 font-sans">
      {/* Sidebar */}
      <aside className="w-56 flex-shrink-0 bg-white border-r border-gray-200 flex flex-col">
        <div className="flex items-center gap-2 px-5 py-4 border-b border-gray-200">
          <HardDrive className="text-indigo-600" size={22} />
          <span className="font-bold text-lg tracking-tight text-gray-900">JiHuan</span>
        </div>
        <nav className="flex-1 py-3 space-y-0.5 px-2">
          {nav.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors',
                  isActive
                    ? 'bg-indigo-50 text-indigo-700 font-medium'
                    : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                )
              }
            >
              <Icon size={16} />
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="px-2 pb-3">
          <button
            onClick={handleLogout}
            className="w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm text-gray-500 hover:bg-gray-100 hover:text-gray-900 transition-colors"
          >
            <LogOut size={16} />
            退出登录
          </button>
        </div>
        <div className="px-5 py-3 border-t border-gray-200 text-xs text-gray-400">
          JiHuan Storage
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-auto">
        <div className="p-6 max-w-6xl mx-auto">{children}</div>
      </main>
    </div>
  )
}
