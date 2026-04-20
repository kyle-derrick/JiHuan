const API_KEY_STORAGE = 'jihuan_api_key'

export function getStoredApiKey(): string {
  return localStorage.getItem(API_KEY_STORAGE) ?? ''
}

export function setStoredApiKey(key: string) {
  localStorage.setItem(API_KEY_STORAGE, key)
}

async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const key = getStoredApiKey()
  const headers: Record<string, string> = { ...(init.headers as Record<string, string>) }
  if (key) headers['X-API-Key'] = key
  const resp = await fetch(path, { ...init, headers })
  return resp
}

async function apiJSON<T>(path: string, init: RequestInit = {}): Promise<T> {
  const resp = await apiFetch(path, init)
  if (!resp.ok) {
    const text = await resp.text()
    let msg = text
    try { msg = JSON.parse(text).error ?? text } catch {}
    throw new Error(`HTTP ${resp.status}: ${msg}`)
  }
  return resp.json() as Promise<T>
}

// ── Status ──────────────────────────────────────────────────────────────────

export interface StatusResponse {
  file_count: number
  block_count: number
  version: string
  hash_algorithm: string
  compression_algorithm: string
  compression_level: number
}

export const getStatus = () => apiJSON<StatusResponse>('/api/status')

// ── Files ───────────────────────────────────────────────────────────────────

export interface FileMeta {
  file_id: string
  file_name: string
  file_size: number
  create_time: number
  content_type: string | null
  chunk_count: number
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
}

export interface BlockListResponse {
  blocks: BlockInfo[]
  count: number
}

export const listBlocks = () => apiJSON<BlockListResponse>('/api/block/list')

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

// ── API Keys ────────────────────────────────────────────────────────────────

export interface KeyInfo {
  key_id: string
  name: string
  key_prefix: string
  created_at: number
  last_used_at: number
  enabled: boolean
}

export interface KeyListResponse {
  keys: KeyInfo[]
  count: number
}

export interface CreateKeyResponse extends KeyInfo {
  key: string
}

export const listKeys = () => apiJSON<KeyListResponse>('/api/keys')

export const createKey = (name: string) =>
  apiJSON<CreateKeyResponse>('/api/keys', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
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
