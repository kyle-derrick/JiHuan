const API_KEY_STORAGE = 'jihuan_api_key'

export function getStoredApiKey(): string {
  return localStorage.getItem(API_KEY_STORAGE) ?? ''
}

export function setStoredApiKey(key: string) {
  localStorage.setItem(API_KEY_STORAGE, key)
}

/** Paths that must never trigger a redirect-to-login on 401, because
 *  they're the auth endpoints themselves or the login page is already showing. */
const AUTH_PATHS = ['/api/auth/login', '/api/auth/logout', '/api/auth/me']

/** Centralized 401 handler: jump to /ui/login while preserving the current
 *  path so we can return there after successful login. Called by apiJSON. */
function redirectToLogin() {
  if (typeof window === 'undefined') return
  if (window.location.pathname === '/ui/login') return
  const next = encodeURIComponent(window.location.pathname + window.location.search)
  window.location.replace(`/ui/login?next=${next}`)
}

async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const key = getStoredApiKey()
  const headers: Record<string, string> = { ...(init.headers as Record<string, string>) }
  // Send the raw API key header *only* when the user has explicitly pasted
  // one into localStorage. In the cookie-login flow this stays empty and the
  // browser attaches `jh_session` automatically (credentials: 'same-origin'
  // is the default for same-origin fetch).
  if (key) headers['X-API-Key'] = key
  const resp = await fetch(path, { ...init, headers, credentials: 'same-origin' })
  return resp
}

async function apiJSON<T>(path: string, init: RequestInit = {}): Promise<T> {
  const resp = await apiFetch(path, init)
  if (!resp.ok) {
    const text = await resp.text()
    let msg = text
    try { msg = JSON.parse(text).error ?? text } catch {}
    if (resp.status === 401 && !AUTH_PATHS.some((p) => path.startsWith(p))) {
      // Trigger navigation to /ui/login and park this request. If we threw
      // here the calling component would briefly render a scary
      // "HTTP 401: Missing or invalid credentials…" message before the
      // browser actually navigates. A pending promise that never resolves
      // keeps the component in its loading state until the redirect wins.
      redirectToLogin()
      return new Promise<T>(() => {})
    }
    throw new Error(`HTTP ${resp.status}: ${msg}`)
  }
  return resp.json() as Promise<T>
}

// ── Status ──────────────────────────────────────────────────────────────────

export interface StatusResponse {
  file_count: number
  block_count: number
  /** Sum of user-uploaded file sizes (pre-compression, pre-dedup). */
  logical_bytes: number
  /** Actual bytes on disk under data_dir (compressed + dedup + block overhead). */
  disk_usage_bytes: number
  /** Configured storage cap; null = unlimited. */
  max_storage_bytes: number | null
  dedup_ratio: number
  uptime_secs: number
  version: string
  hash_algorithm: string
  compression_algorithm: string
  compression_level: number
}

export const getStatus = () => apiJSON<StatusResponse>('/api/status')

// ── Files ───────────────────────────────────────────────────────────────────

export interface ChunkInfo {
  index: number
  block_id: string
  offset: number
  original_size: number
  compressed_size: number
  hash: string
}

export interface FileMeta {
  file_id: string
  file_name: string
  file_size: number
  create_time: number
  content_type: string | null
  chunk_count: number
  chunks?: ChunkInfo[]
  partition_id?: number
}

export interface UploadResponse {
  file_id: string
  file_name: string
  file_size: number
}

export async function uploadFile(file: File, onProgress?: (pct: number) => void): Promise<UploadResponse> {
  const form = new FormData()
  form.append('file', file)
  // XHR for progress
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('POST', '/api/v1/files')
    const key = getStoredApiKey()
    if (key) xhr.setRequestHeader('X-API-Key', key)
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && onProgress) onProgress(Math.round((e.loaded / e.total) * 100))
    }
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(JSON.parse(xhr.responseText))
      } else {
        let msg = xhr.responseText
        try { msg = JSON.parse(xhr.responseText).error ?? msg } catch {}
        reject(new Error(`HTTP ${xhr.status}: ${msg}`))
      }
    }
    xhr.onerror = () => reject(new Error('Network error'))
    xhr.send(form)
  })
}

export interface FileListResponse {
  files: FileMeta[]
  count: number
  total: number
}

export interface ListFilesParams {
  limit?: number
  offset?: number
  q?: string
  sort?: 'create_time' | 'file_name' | 'file_size'
  order?: 'asc' | 'desc'
}

export const listFiles = (params: ListFilesParams = {}) => {
  const sp = new URLSearchParams()
  if (params.limit != null) sp.set('limit', String(params.limit))
  if (params.offset != null) sp.set('offset', String(params.offset))
  if (params.q) sp.set('q', params.q)
  if (params.sort) sp.set('sort', params.sort)
  if (params.order) sp.set('order', params.order)
  const qs = sp.toString()
  return apiJSON<FileListResponse>(`/api/v1/files${qs ? '?' + qs : ''}`)
}

export const getFileMeta = (fileId: string) =>
  apiJSON<FileMeta>(`/api/v1/files/${fileId}/meta`)

export async function downloadFile(fileId: string, fileName: string) {
  const resp = await apiFetch(`/api/v1/files/${fileId}`)
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
  const blob = await resp.blob()
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = fileName
  a.click()
  URL.revokeObjectURL(url)
}

export async function deleteFile(fileId: string) {
  const resp = await apiFetch(`/api/v1/files/${fileId}`, { method: 'DELETE' })
  if (!resp.ok) {
    const text = await resp.text()
    let msg = text
    try { msg = JSON.parse(text).error ?? text } catch {}
    throw new Error(`HTTP ${resp.status}: ${msg}`)
  }
}

// ── Blocks ──────────────────────────────────────────────────────────────────

export interface BlockInfo {
  block_id: string
  size: number
  ref_count: number
  path: string
  create_time: number
  sealed: boolean
}

export interface BlockListResponse {
  blocks: BlockInfo[]
  count: number
}

export interface ReferencingFile {
  file_id: string
  file_name: string
  file_size: number
  chunks_in_block: number
}

export interface BlockDetail extends BlockInfo {
  referencing_files: ReferencingFile[]
}

export const listBlocks = () => apiJSON<BlockListResponse>('/api/block/list')

export const getBlockDetail = (blockId: string) =>
  apiJSON<BlockDetail>(`/api/block/${blockId}`)

export async function deleteBlock(blockId: string) {
  const resp = await apiFetch(`/api/block/${blockId}`, { method: 'DELETE' })
  if (!resp.ok && resp.status !== 204) {
    const text = await resp.text()
    let msg = text
    try { msg = JSON.parse(text).error ?? text } catch {}
    throw new Error(`HTTP ${resp.status}: ${msg}`)
  }
}

// ── Config ──────────────────────────────────────────────────────────────────

export const getConfig = () => apiJSON<Record<string, unknown>>('/api/config')

// ── Metrics ─────────────────────────────────────────────────────────────────

/** Fetch raw Prometheus metrics text via the same-origin proxy endpoint.
 *
 * The server re-exposes the Prometheus exposition format at `/api/metrics`
 * (in addition to the standalone `:9090/metrics` listener used by Prometheus
 * scraping). Using the same origin avoids CORS/firewall issues for the UI. */
export async function fetchMetricsText(): Promise<string> {
  const resp = await apiFetch('/api/metrics')
  if (!resp.ok) throw new Error(`Metrics endpoint returned HTTP ${resp.status}`)
  return resp.text()
}

export interface ParsedMetrics {
  counters: Record<string, number>
  histograms: Record<string, { count: number; sum: number; quantiles: Record<string, number> }>
}

/** Minimal Prometheus text-format parser for our known metrics. */
export function parseMetrics(text: string): ParsedMetrics {
  const counters: Record<string, number> = {}
  const histograms: Record<string, { count: number; sum: number; quantiles: Record<string, number> }> = {}
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue
    // counter: name value
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE+.\-]+)/)
    if (!m) continue
    const [, name, labels = '', valueStr] = m
    const value = parseFloat(valueStr)
    if (!Number.isFinite(value)) continue
    if (name.endsWith('_count') || name.endsWith('_sum')) {
      const base = name.replace(/_(count|sum)$/, '')
      if (!histograms[base]) histograms[base] = { count: 0, sum: 0, quantiles: {} }
      if (name.endsWith('_count')) histograms[base].count = value
      else histograms[base].sum = value
    } else if (labels.includes('quantile=')) {
      const qm = labels.match(/quantile="([^"]+)"/)
      if (qm) {
        if (!histograms[name]) histograms[name] = { count: 0, sum: 0, quantiles: {} }
        histograms[name].quantiles[qm[1]] = value
      }
    } else if (!labels) {
      counters[name] = value
    }
  }
  return { counters, histograms }
}

// ── GC ──────────────────────────────────────────────────────────────────────

export interface GcResponse {
  blocks_deleted: number
  bytes_reclaimed: number
  partitions_deleted: number
  files_deleted: number
  duration_ms: number
}

export const triggerGc = () =>
  apiJSON<GcResponse>('/api/gc/trigger', { method: 'POST' })

// ── Compaction (v0.4.4) ─────────────────────────────────────────────────────

export interface CompactionBlockStats {
  old_block_id: string
  new_block_id: string | null
  old_size_bytes: number
  new_size_bytes: number
  bytes_saved: number
  live_chunks: number
  dropped_chunks: number
}

export interface CompactResponse {
  compacted: CompactionBlockStats[]
  total_bytes_saved: number
}

export interface CompactRequest {
  block_id?: string
  threshold?: number
  min_size_bytes?: number
}

/** POST /api/admin/compact — explicit block_id takes priority over scan. */
export const compactBlocks = (req: CompactRequest = {}) =>
  apiJSON<CompactResponse>('/api/admin/compact', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })

export interface SealResponse {
  sealed_block_id: string | null
  size: number
}

/** POST /api/admin/seal — force-seal the current active block so it becomes
 * eligible for compaction / GC. Returns null sealed_block_id when the active
 * writer was already empty (nothing to seal). */
export const sealActiveBlock = () =>
  apiJSON<SealResponse>('/api/admin/seal', { method: 'POST' })

// ── API Keys ────────────────────────────────────────────────────────────────

export interface KeyInfo {
  key_id: string
  name: string
  key_prefix: string
  created_at: number
  last_used_at: number
  enabled: boolean
  scopes: string[]
}

export interface KeyListResponse {
  keys: KeyInfo[]
  count: number
}

export interface CreateKeyResponse extends KeyInfo {
  key: string
}

export const listKeys = () => apiJSON<KeyListResponse>('/api/keys')

export const createKey = (name: string, scopes?: string[]) =>
  apiJSON<CreateKeyResponse>('/api/keys', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, scopes }),
  })

export async function deleteKey(keyId: string) {
  const resp = await apiFetch(`/api/keys/${keyId}`, { method: 'DELETE' })
  if (!resp.ok && resp.status !== 204) {
    const text = await resp.text()
    let msg = text
    try { msg = JSON.parse(text).error ?? text } catch {}
    throw new Error(`HTTP ${resp.status}: ${msg}`)
  }
}

// ── Session auth (cookie-based) ─────────────────────────────────────────────

export interface LoginResponse {
  key_id: string
  name: string
  scopes: string[]
  expires_in: number
}

/** Exchange an API key for an HttpOnly `jh_session` cookie. */
export const login = (key: string) =>
  apiJSON<LoginResponse>('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key }),
  })

/** Clear the session cookie. Always resolves (204 is expected). */
export async function logout() {
  await apiFetch('/api/auth/logout', { method: 'POST' })
}

/** Returns the authenticated caller's `KeyInfo`, or throws on 401.
 *  Unlike other calls, this one does NOT auto-redirect on 401 — the caller
 *  (AuthGuard) uses the thrown error to decide. */
export const getMe = () => apiJSON<KeyInfo>('/api/auth/me')

/** Rotate the caller's own credential. Returns on 204; throws on error.
 *  The existing session cookie remains valid afterwards (session is bound
 *  to key_id, not to the hash). */
export async function changePassword(newPassword: string) {
  const resp = await apiFetch('/api/auth/change-password', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ new_password: newPassword }),
  })
  if (resp.status === 204) return
  const text = await resp.text()
  let msg = text
  try { msg = JSON.parse(text).error ?? text } catch {}
  throw new Error(`HTTP ${resp.status}: ${msg}`)
}

// ── Audit log (Phase 2.6) ────────────────────────────────────────────────────

/** Matches `jihuan_core::metadata::types::AuditResult`.
 *  Serde serializes this enum with `#[serde(tag = "kind", rename_all = "snake_case")]`. */
export type AuditResult =
  | { kind: 'ok' }
  | { kind: 'denied'; reason: string }
  | { kind: 'error'; message: string }

/** Matches `jihuan_core::metadata::types::AuditEvent`. */
export interface AuditEvent {
  ts: number
  actor_key_id: string | null
  actor_ip: string | null
  action: string
  target: string | null
  result: AuditResult
  http_status: number | null
}

export interface AuditListResponse {
  events: AuditEvent[]
  count: number
}

export interface AuditQuery {
  since?: number
  until?: number
  /** Exact match against `actor_key_id`. */
  actor?: string
  /** Prefix match on `action` (e.g. `auth.` returns every auth event). */
  action?: string
  /** Page size cap. Default 200, max 1000 (server-enforced). */
  limit?: number
}

export function listAudit(q: AuditQuery = {}): Promise<AuditListResponse> {
  const params = new URLSearchParams()
  if (q.since != null) params.set('since', String(q.since))
  if (q.until != null) params.set('until', String(q.until))
  if (q.actor) params.set('actor', q.actor)
  if (q.action) params.set('action', q.action)
  if (q.limit != null) params.set('limit', String(q.limit))
  const qs = params.toString()
  return apiJSON<AuditListResponse>(`/api/admin/audit${qs ? `?${qs}` : ''}`)
}
