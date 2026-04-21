import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from '@/components/Layout'
import Dashboard from '@/pages/Dashboard'
import Files from '@/pages/Files'
import Blocks from '@/pages/Blocks'
import GC from '@/pages/GC'
import Keys from '@/pages/Keys'
import Settings from '@/pages/Settings'
import Metrics from '@/pages/Metrics'
import Login from '@/pages/Login'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Login sits outside the Layout — no sidebar / nav. */}
        <Route path="/ui/login" element={<Login />} />

        {/* Everything else lives inside Layout. 401 handling is centralized
            in api.ts (redirectToLogin), so no explicit auth guard is needed
            here — any protected page that throws 401 on load will trigger
            a navigation to /ui/login?next=… */}
        <Route
          path="/ui/*"
          element={
            <Layout>
              <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/files" element={<Files />} />
                <Route path="/blocks" element={<Blocks />} />
                <Route path="/gc" element={<GC />} />
                <Route path="/keys" element={<Keys />} />
                <Route path="/settings" element={<Settings />} />
                <Route path="/metrics" element={<Metrics />} />
                <Route path="*" element={<Navigate to="/ui/" replace />} />
              </Routes>
            </Layout>
          }
        />
        <Route path="*" element={<Navigate to="/ui/" replace />} />
      </Routes>
    </BrowserRouter>
  )
}
