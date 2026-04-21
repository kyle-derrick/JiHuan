import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from '@/components/Layout'
import Dashboard from '@/pages/Dashboard'
import Files from '@/pages/Files'
import Blocks from '@/pages/Blocks'
import GC from '@/pages/GC'
import Keys from '@/pages/Keys'
import Settings from '@/pages/Settings'
import Metrics from '@/pages/Metrics'

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/ui/" element={<Dashboard />} />
          <Route path="/ui/files" element={<Files />} />
          <Route path="/ui/blocks" element={<Blocks />} />
          <Route path="/ui/gc" element={<GC />} />
          <Route path="/ui/keys" element={<Keys />} />
          <Route path="/ui/settings" element={<Settings />} />
          <Route path="/ui/metrics" element={<Metrics />} />
          <Route path="*" element={<Navigate to="/ui/" replace />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}
