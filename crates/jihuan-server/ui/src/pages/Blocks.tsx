import { useEffect, useMemo, useState } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { RefreshCw, Trash2, Eye, X, Trash, Shrink, Lock as LockIcon, Info, ChevronDown, ChevronUp, ArrowUp, ArrowDown, ArrowUpDown } from 'lucide-react'
import {
  listBlocks, getBlockDetail, deleteBlock, triggerGc, compactBlocks, sealActiveBlock,
  type BlockInfo, type BlockDetail,
} from '@/api'
import { formatBytes, formatUnixTime } from '@/lib/utils'

export default function Blocks() {
  const [blocks, setBlocks] = useState<BlockInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [info, setInfo] = useState('')
  const [detail, setDetail] = useState<BlockDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [gcing, setGcing] = useState(false)
  const [compacting, setCompacting] = useState(false)
  const [sealing, setSealing] = useState(false)
  const [helpOpen, setHelpOpen] = useState(false)
  const [searchParams, setSearchParams] = useSearchParams()

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const r = await listBlocks()
      setBlocks(r.blocks)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  const showDetail = async (blockId: string) => {
    setDetailLoading(true)
    try {
      const d = await getBlockDetail(blockId)
      setDetail(d)
    } catch (e) {
      setError(String(e))
    } finally {
      setDetailLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  // Handle ?block=<id> deep link from Files page
  useEffect(() => {
    const bid = searchParams.get('block')
    if (bid) {
      showDetail(bid)
      searchParams.delete('block')
      setSearchParams(searchParams, { replace: true })
    }
  }, [searchParams])

  const handleDelete = async (blockId: string) => {
    if (!confirm('确认删除该 Block？仅当 ref_count=0 时允许。')) return
    try {
      await deleteBlock(blockId)
      setInfo(`已删除 Block ${blockId.slice(0, 12)}…`)
      setDetail(null)
      await load()
    } catch (e) {
      setError(String(e))
    }
  }

  const handleGc = async () => {
    setGcing(true)
    setError('')
    try {
      const r = await triggerGc()
      setInfo(`GC 完成：删除 ${r.blocks_deleted} 个 Block，回收 ${formatBytes(r.bytes_reclaimed)}，耗时 ${r.duration_ms}ms`)
      await load()
    } catch (e) {
      setError(String(e))
    } finally {
      setGcing(false)
    }
  }

  /** v0.4.4: force-seal the active block so it becomes compaction-eligible. */
  const handleSeal = async () => {
    setSealing(true)
    setError('')
    try {
      const r = await sealActiveBlock()
      if (r.sealed_block_id) {
        setInfo(
          `已封存 Block ${r.sealed_block_id.slice(0, 12)}…（${formatBytes(r.size)}），现在可以压实了`,
        )
      } else {
        setInfo('当前无活跃 Block，无需封存')
      }
      await load()
    } catch (e) {
      setError(String(e))
    } finally {
      setSealing(false)
    }
  }

  /** v0.4.4: trigger compaction. Without block_id → scan low-utilisation. */
  const handleCompact = async (blockId?: string) => {
    const label = blockId ? `Block ${blockId.slice(0, 8)}…` : '低利用率 Block'
    setCompacting(true)
    setError('')
    try {
      // NB: min_size_bytes=0 so that typical dev/small workloads (where
      // blocks rarely exceed a few KiB before sealing) are not silently
      // filtered out. The real gate is the utilisation threshold.
      const r = await compactBlocks(
        blockId
          ? { block_id: blockId }
          : { threshold: 0.5, min_size_bytes: 0 },
      )
      if (r.compacted.length === 0) {
        // v0.4.5: /api/admin/compact now auto-seals a dead active block
        // before scanning, so hitting this branch really does mean every
        // sealed block is above threshold. No more "please seal first"
        // nudge — the backend handles it.
        setInfo('压实完成：没有块的利用率低于 50%，当前空间已足够紧凑')
      } else {
        const savedStr = formatBytes(Math.abs(r.total_bytes_saved))
        const sign = r.total_bytes_saved >= 0 ? '节省' : '增长'
        setInfo(
          `压实完成：${r.compacted.length} 个 Block，共 ${sign} ${savedStr}`,
        )
      }
      setDetail(null)
      await load()
    } catch (e) {
      setError(String(e))
    } finally {
      setCompacting(false)
    }
  }

  const totalSize = blocks.reduce((acc, b) => acc + b.size, 0)
  const totalLive = blocks.reduce((acc, b) => acc + (b.live_bytes ?? 0), 0)
  const activeCount = blocks.filter(b => !b.sealed).length
  const orphanCount = blocks.filter(b => b.ref_count === 0).length
  const lowUtilCount = blocks.filter(b => b.sealed && b.utilization != null && b.utilization < 0.5).length

  /** Colour utilisation % so the operator can spot compaction candidates at a glance. */
  const utilColor = (u: number | null) => {
    if (u == null) return 'text-gray-400'
    if (u < 0.5) return 'text-red-700'
    if (u < 0.8) return 'text-amber-700'
    return 'text-emerald-700'
  }
  const formatUtil = (u: number | null) =>
    u == null ? '—' : `${(u * 100).toFixed(1)}%`

  // ── Sorting ───────────────────────────────────────────────────────────
  // Operators specifically want "utilisation ascending" to find compaction
  // candidates. We support the common columns via a tiny click-to-sort
  // model: null key = insertion order (server-side ordering preserved).
  type SortKey = 'block_id' | 'size' | 'live_bytes' | 'utilization' | 'ref_count' | 'create_time'
  type SortDir = 'asc' | 'desc'
  const [sortKey, setSortKey] = useState<SortKey | null>(null)
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const toggleSort = (k: SortKey) => {
    if (sortKey !== k) {
      setSortKey(k)
      // utilisation defaults to asc (low-util first = most compactable);
      // size/ref_count/create_time default to desc (biggest/newest first).
      setSortDir(k === 'utilization' || k === 'block_id' ? 'asc' : 'desc')
    } else if (sortDir === 'asc') {
      setSortDir('desc')
    } else {
      // Third click resets to server order.
      setSortKey(null)
      setSortDir('asc')
    }
  }

  const sortedBlocks = useMemo(() => {
    if (!sortKey) return blocks
    const mult = sortDir === 'asc' ? 1 : -1
    // unsealed (utilization == null) sort as -Infinity asc / +Infinity desc
    // so they cluster predictably at one end and never poison the numeric
    // ordering of sealed rows.
    const utilSort = (u: number | null) =>
      u == null ? (sortDir === 'asc' ? -Infinity : Infinity) : u
    const out = [...blocks]
    out.sort((a, b) => {
      switch (sortKey) {
        case 'block_id':    return mult * a.block_id.localeCompare(b.block_id)
        case 'size':        return mult * (a.size - b.size)
        case 'live_bytes':  return mult * ((a.live_bytes ?? 0) - (b.live_bytes ?? 0))
        case 'utilization': return mult * (utilSort(a.utilization ?? null) - utilSort(b.utilization ?? null))
        case 'ref_count':   return mult * (a.ref_count - b.ref_count)
        case 'create_time': return mult * (a.create_time - b.create_time)
      }
    })
    return out
  }, [blocks, sortKey, sortDir])

  const SortIndicator = ({ k }: { k: SortKey }) => {
    if (sortKey !== k) return <ArrowUpDown size={12} className="inline-block ml-1 text-gray-300" />
    return sortDir === 'asc'
      ? <ArrowUp size={12} className="inline-block ml-1 text-gray-700" />
      : <ArrowDown size={12} className="inline-block ml-1 text-gray-700" />
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Block 管理</h1>
        <div className="flex gap-2">
          <button
            onClick={handleSeal}
            disabled={sealing || activeCount === 0}
            title={
              activeCount === 0
                ? '当前无活跃 Block'
                : '封存活跃 Block（使其可被压实 / GC 回收）'
            }
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-sky-700 bg-sky-50 border border-sky-200 rounded-lg hover:bg-sky-100 disabled:opacity-50"
          >
            <LockIcon size={14} className={sealing ? 'animate-spin' : ''} />
            {sealing ? '封存中…' : '封存活跃 Block'}
          </button>
          <button
            onClick={() => handleCompact()}
            disabled={compacting}
            title={
              '扫描并压实利用率低于 50% 的已封存 Block。\n' +
              '· 命中的块按活数据大小贪心装箱，合并写入尽可能少的新 Block\n' +
              '· N 个低利用率块 → ≤ N 个新块（受 block_file_size 限制）\n' +
              '· 每个合并组是一次 redb 事务，全部成功或全部不变\n' +
              '· 活跃 Block 默认跳过；若它已无活引用，后端会先自动封存再合并'
            }
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-indigo-700 bg-indigo-50 border border-indigo-200 rounded-lg hover:bg-indigo-100 disabled:opacity-50"
          >
            <Shrink size={14} className={compacting ? 'animate-spin' : ''} />
            {compacting ? '压实中…' : '压实低利用率'}
          </button>
          <button
            onClick={handleGc}
            disabled={gcing}
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-amber-700 bg-amber-50 border border-amber-200 rounded-lg hover:bg-amber-100 disabled:opacity-50"
          >
            <Trash size={14} className={gcing ? 'animate-spin' : ''} />
            {gcing ? 'GC 运行中…' : '触发 GC'}
          </button>
          <button
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-2 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            刷新
          </button>
        </div>
      </div>

      {/* v0.4.5: make the compact / GC strategy explicit in-UI so operators
          don't expect cross-block merging. Collapsed by default to keep the
          toolbar uncluttered. */}
      <div className="mb-4 border border-gray-200 rounded-lg bg-white">
        <button
          type="button"
          onClick={() => setHelpOpen(v => !v)}
          className="w-full flex items-center justify-between px-3 py-2 text-sm text-gray-700 hover:bg-gray-50 rounded-lg"
        >
          <span className="flex items-center gap-2">
            <Info size={14} className="text-gray-500" />
            <span>压实 / GC 策略说明（点击展开）</span>
          </span>
          {helpOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
        </button>
        {helpOpen && (
          <div className="px-4 pb-4 pt-1 text-sm text-gray-700 leading-relaxed space-y-3 border-t border-gray-100">
            <div>
              <div className="font-semibold text-gray-900 mb-1">1. 压实是"重写 + 跨块合并"</div>
              <p>
                每次"压实低利用率"会扫描所有利用率低于 50% 的 sealed Block，
                按各块的活数据量<b>升序贪心装箱</b>：多个小块的活 chunks 被<b>合并写入同一个新 Block</b>，
                直到其活数据总量接近 <code>block_file_size</code> 上限才开启下一个新块。
                每个合并组是一次 redb 事务，要么全部生效、要么全部不变 —— 崩溃恢复路径和单块重写一致（孤儿新块下次 GC 清理）。
              </p>
            </div>
            <div>
              <div className="font-semibold text-gray-900 mb-1">2. 活跃 Block 的处理</div>
              <p>
                活跃 Block 通常不参与压实/GC（它随时在被写入）。例外：
                <b>若活跃块的 ref_count == 0</b>（例如删除了里面唯一一个文件），
                点"压实低利用率"或"触发 GC"时，后端会先自动封存它，然后正常合并 —— 无需手动先点"封存活跃 Block"。
              </p>
            </div>
            <div>
              <div className="font-semibold text-gray-900 mb-1">3. 单块定向压实（指定 block_id）</div>
              <p>
                如果通过 <code>/api/admin/compact</code> 指定 <code>block_id</code>，则走<b>单块重写</b>路径：
                只丢弃该块的死 chunks，不跨块合并。这是对某个特定块精准操作的场景。
              </p>
            </div>
            <div>
              <div className="font-semibold text-gray-900 mb-1">4. 阈值与最小尺寸语义</div>
              <p>
                · <b>threshold</b>：块利用率 <code>live_bytes / size</code> 低于此值才算候选。你的 64% 利用率块在 0.5 下会被跳过，要调到 0.8+ 才能触发。
                <br />· <b>min_size_bytes</b>：小于该值的块直接跳过（UI 默认 0 不过滤；CLI/HTTP 默认 4 MiB）。
                <br />· <b>block_file_size</b>：合并组输出块的软上限。越大越容易把小块全部装进同一个新块。
              </p>
            </div>
          </div>
        )}
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-200 flex justify-between">
          <span>{error}</span>
          <button onClick={() => setError('')}><X size={14} /></button>
        </div>
      )}
      {info && (
        <div className="mb-4 p-3 bg-green-50 text-green-700 rounded-lg text-sm border border-green-200 flex justify-between">
          <span>{info}</span>
          <button onClick={() => setInfo('')}><X size={14} /></button>
        </div>
      )}

      <div className="mb-4 flex gap-4 text-sm text-gray-500 flex-wrap">
        <span>共 <strong className="text-gray-900">{blocks.length}</strong> 个 Block</span>
        <span>总大小 <strong className="text-gray-900">{formatBytes(totalSize)}</strong></span>
        <span title="所有块中唯一活 chunk 的 compressed_size 之和，与压实扫描使用的口径一致">
          活数据 <strong className="text-gray-900">{formatBytes(totalLive)}</strong>
          {totalSize > 0 && (
            <span className="text-gray-400"> （整体 {(totalLive / totalSize * 100).toFixed(1)}%）</span>
          )}
        </span>
        <span>活跃（未 seal）<strong className="text-amber-700">{activeCount}</strong></span>
        <span>孤立（ref=0）<strong className="text-red-700">{orphanCount}</strong></span>
        <span title="已封存且利用率 < 50% 的块数量，即压实候选">
          低利用率 <strong className="text-red-700">{lowUtilCount}</strong>
        </span>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left">
              <th
                className="px-4 py-3 font-medium text-gray-500 cursor-pointer select-none hover:text-gray-900"
                onClick={() => toggleSort('block_id')}
              >
                Block ID<SortIndicator k="block_id" />
              </th>
              <th className="px-4 py-3 font-medium text-gray-500">状态</th>
              <th
                className="px-4 py-3 font-medium text-gray-500 cursor-pointer select-none hover:text-gray-900"
                onClick={() => toggleSort('size')}
              >
                大小<SortIndicator k="size" />
              </th>
              <th
                className="px-4 py-3 font-medium text-gray-500 cursor-pointer select-none hover:text-gray-900"
                onClick={() => toggleSort('live_bytes')}
                title="活 chunk compressed_size 合计（按唯一 hash 去重）"
              >
                活数据<SortIndicator k="live_bytes" />
              </th>
              <th
                className="px-4 py-3 font-medium text-gray-500 cursor-pointer select-none hover:text-gray-900"
                onClick={() => toggleSort('utilization')}
                title="live_bytes / size  •  点击按升序排序，最低利用率的块排在最前（压实候选）"
              >
                利用率<SortIndicator k="utilization" />
              </th>
              <th
                className="px-4 py-3 font-medium text-gray-500 cursor-pointer select-none hover:text-gray-900"
                onClick={() => toggleSort('ref_count')}
              >
                引用数<SortIndicator k="ref_count" />
              </th>
              <th
                className="px-4 py-3 font-medium text-gray-500 cursor-pointer select-none hover:text-gray-900"
                onClick={() => toggleSort('create_time')}
              >
                创建时间<SortIndicator k="create_time" />
              </th>
              <th className="px-4 py-3 font-medium text-gray-500 w-28">操作</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={8} className="px-4 py-10 text-center text-gray-400">加载中…</td></tr>
            )}
            {!loading && blocks.length === 0 && (
              <tr><td colSpan={8} className="px-4 py-10 text-center text-gray-400">暂无 Block</td></tr>
            )}
            {sortedBlocks.map((b) => (
              <tr key={b.block_id} className="border-b border-gray-50 hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs text-gray-700">{b.block_id}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    b.sealed ? 'bg-blue-50 text-blue-700' : 'bg-amber-50 text-amber-700'
                  }`}>
                    {b.sealed ? 'sealed' : 'active'}
                  </span>
                </td>
                <td className="px-4 py-3 text-gray-600">{formatBytes(b.size)}</td>
                <td className="px-4 py-3 text-gray-600">{formatBytes(b.live_bytes ?? 0)}</td>
                <td className={`px-4 py-3 font-medium ${utilColor(b.utilization ?? null)}`}>
                  {formatUtil(b.utilization ?? null)}
                </td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    b.ref_count === 0 ? 'bg-red-50 text-red-600' : 'bg-green-50 text-green-700'
                  }`}>
                    {b.ref_count}
                  </span>
                </td>
                <td className="px-4 py-3 text-gray-500 text-xs">{formatUnixTime(b.create_time)}</td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => showDetail(b.block_id)}
                      className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded"
                      title="详情"
                    >
                      <Eye size={14} />
                    </button>
                    <button
                      onClick={() => handleCompact(b.block_id)}
                      disabled={!b.sealed || b.ref_count === 0 || compacting}
                      className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-gray-400"
                      title={
                        !b.sealed
                          ? '活跃 Block 不可压实'
                          : b.ref_count === 0
                            ? '无活引用 — 直接走 GC 即可'
                            : '压实此 Block（丢弃已删除内容）'
                      }
                    >
                      <Shrink size={14} />
                    </button>
                    <button
                      onClick={() => handleDelete(b.block_id)}
                      disabled={b.ref_count > 0}
                      className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-gray-400"
                      title={b.ref_count > 0 ? `有 ${b.ref_count} 个引用，无法删除` : '回收（ref_count=0）'}
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Detail modal */}
      {(detail || detailLoading) && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4" onClick={() => { setDetail(null); setDetailLoading(false) }}>
          <div className="bg-white rounded-2xl p-6 w-full max-w-3xl max-h-[85vh] overflow-auto shadow-xl" onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold">Block 详情</h2>
              <button onClick={() => { setDetail(null); setDetailLoading(false) }} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
            </div>

            {detailLoading && !detail && <div className="text-center text-gray-400 py-8">加载中…</div>}

            {detail && (
              <>
                <dl className="space-y-1 text-sm mb-5">
                  {[
                    ['Block ID', detail.block_id],
                    ['状态', detail.sealed ? 'sealed（已封存）' : 'active（写入中）'],
                    ['大小', formatBytes(detail.size)],
                    ['活数据', `${formatBytes(detail.live_bytes ?? 0)}  ·  利用率 ${formatUtil(detail.utilization ?? null)}`],
                    ['引用数', String(detail.ref_count)],
                    ['路径', detail.path],
                    ['创建时间', formatUnixTime(detail.create_time)],
                  ].map(([k, v]) => (
                    <div key={k} className="flex justify-between py-1.5 border-b border-gray-100">
                      <dt className="text-gray-500">{k}</dt>
                      <dd className="font-mono text-gray-800 break-all max-w-xl text-right">{v}</dd>
                    </div>
                  ))}
                </dl>

                <h3 className="text-sm font-semibold text-gray-700 mb-2">
                  引用此 Block 的文件（{detail.referencing_files.length}）
                </h3>
                {detail.referencing_files.length === 0 ? (
                  <div className="text-center text-gray-400 py-4 text-sm border border-dashed border-gray-200 rounded-lg">
                    无文件引用此 Block — 可安全删除
                  </div>
                ) : (
                  <div className="overflow-auto border border-gray-100 rounded-lg">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-50">
                        <tr className="text-left text-gray-500">
                          <th className="px-3 py-2">文件名</th>
                          <th className="px-3 py-2">文件大小</th>
                          <th className="px-3 py-2">块内 Chunks</th>
                          <th className="px-3 py-2">File ID</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.referencing_files.map((f) => (
                          <tr key={f.file_id} className="border-t border-gray-100 hover:bg-gray-50">
                            <td className="px-3 py-1.5 text-gray-900">{f.file_name}</td>
                            <td className="px-3 py-1.5 text-gray-600">{formatBytes(f.file_size)}</td>
                            <td className="px-3 py-1.5 text-gray-600">{f.chunks_in_block}</td>
                            <td className="px-3 py-1.5 font-mono text-gray-500">
                              <Link to={`/ui/files`} className="text-indigo-600 hover:underline">
                                {f.file_id.slice(0, 12)}…
                              </Link>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                <div className="mt-5 flex gap-2">
                  <button
                    onClick={() => handleDelete(detail.block_id)}
                    disabled={detail.ref_count > 0}
                    className="flex-1 py-2 text-sm bg-red-50 text-red-700 border border-red-200 rounded-lg hover:bg-red-100 disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    {detail.ref_count > 0 ? `有 ${detail.ref_count} 个引用，无法删除` : '回收此 Block'}
                  </button>
                  <button
                    onClick={() => { setDetail(null); setDetailLoading(false) }}
                    className="flex-1 py-2 text-sm bg-gray-100 rounded-lg hover:bg-gray-200"
                  >关闭</button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
