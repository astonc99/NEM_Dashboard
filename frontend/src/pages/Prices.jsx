import { useEffect, useState, useCallback } from 'react'
import {
  AreaChart, Area, XAxis, YAxis,
  CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer,
} from 'recharts'
import { prices as pricesApi } from '../api/client'
import MetricCard from '../components/MetricCard'
import LoadingSpinner from '../components/LoadingSpinner'
import EmptyState from '../components/EmptyState'
import BackfillPanel from '../components/BackfillPanel'
import TimeRangeButtons, { activePeriodLabel, fmtAxisForPeriod } from '../components/TimeRangeButtons'
import { useSyncStatus } from '../context/SyncContext'

function fmtFull(ts) {
  return new Date(ts).toLocaleString('en-AU', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  })
}

function PriceTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const rrp = payload[0]?.value
  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 shadow-xl">
      <p className="text-slate-400 text-xs mb-1">{fmtFull(label)}</p>
      <p className={`text-sm font-semibold tabular-nums ${rrp < 0 ? 'text-red-400' : 'text-blue-300'}`}>
        ${rrp?.toFixed(2)} /MWh
      </p>
    </div>
  )
}

export default function Prices() {
  const { refreshKey } = useSyncStatus()
  const [meta, setMeta]               = useState(null)
  const [summary, setSummary]         = useState(null)
  const [chartData, setChartData]     = useState(null)
  const [loadingChart, setLoadingChart] = useState(false)
  const [startDate, setStartDate]     = useState('')
  const [endDate, setEndDate]         = useState('')

  useEffect(() => {
    pricesApi.getMeta().then(m => {
      setMeta(m)
      if (m.max_date) {
        const end   = new Date(m.max_date)
        const start = new Date(end.getFullYear(), end.getMonth(), 1)
        setStartDate(start.toISOString().slice(0, 10))
        setEndDate(m.max_date)
      }
    }).catch(() => {})
    pricesApi.getSummary().then(setSummary).catch(() => {})
  }, [refreshKey])

  const fetchChart = useCallback(() => {
    if (!startDate || !endDate) return
    setLoadingChart(true)
    pricesApi
      .getData({ start_date: startDate, end_date: endDate })
      .then(r => setChartData(r.data))
      .catch(() => setChartData([]))
      .finally(() => setLoadingChart(false))
  }, [startDate, endDate])

  useEffect(() => { fetchChart() }, [fetchChart])

  const hasData  = chartData && chartData.length > 0
  const period   = activePeriodLabel(startDate, endDate)

  return (
    <div className="p-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-100">Spot Prices</h1>
        <p className="text-slate-500 text-sm mt-1">NEM Victoria · 5-minute dispatch price (RRP) · A$/MWh</p>
      </div>

      <div className="grid grid-cols-3 gap-3 mb-6">
        <MetricCard
          label="Latest RRP"
          value={summary ? `$${summary.latest_rrp?.toFixed(2)}` : null}
          sub="A$/MWh · VIC1"
          trend={summary?.latest_rrp < 0 ? 'negative' : undefined}
          loading={!summary}
        />
        <MetricCard
          label="Day Average RRP"
          value={summary ? `$${summary.today_avg_rrp?.toFixed(2)}` : null}
          sub={summary?.latest_date ?? ''}
          trend={summary?.today_avg_rrp < 0 ? 'negative' : undefined}
          loading={!summary}
        />
        <MetricCard
          label="Negative Intervals"
          value={summary ? String(summary.negative_intervals_today) : null}
          sub="Today · RRP < $0/MWh"
          trend={summary?.negative_intervals_today > 0 ? 'negative' : undefined}
          loading={!summary}
        />
      </div>

      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 mb-4">
        {/* Chart header — title + period buttons + date pickers */}
        <div className="flex items-start justify-between mb-5 gap-4">
          <div className="flex-shrink-0">
            <h2 className="text-sm font-medium text-slate-200">RRP Over Time</h2>
            {hasData && (
              <p className="text-xs text-slate-600 mt-0.5">{chartData.length.toLocaleString()} intervals</p>
            )}
          </div>
          <div className="flex flex-col items-end gap-2">
            <TimeRangeButtons
              maxDate={meta?.max_date}
              minDate={meta?.min_date}
              startDate={startDate}
              endDate={endDate}
              onSelect={(s, e) => { setStartDate(s); setEndDate(e) }}
            />
            <div className="flex items-center gap-2">
              <input
                type="date"
                value={startDate}
                min={meta?.min_date ?? ''}
                max={endDate || (meta?.max_date ?? '')}
                onChange={e => setStartDate(e.target.value)}
                className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-blue-500/60"
              />
              <span className="text-slate-700 text-xs">to</span>
              <input
                type="date"
                value={endDate}
                min={startDate || (meta?.min_date ?? '')}
                max={meta?.max_date ?? ''}
                onChange={e => setEndDate(e.target.value)}
                className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-blue-500/60"
              />
            </div>
          </div>
        </div>

        {loadingChart ? (
          <LoadingSpinner message="Loading price data..." />
        ) : !hasData ? (
          <EmptyState
            title="No price data for this range"
            description="Use the backfill panel below to download data, then try again."
          />
        ) : (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                <defs>
                  <linearGradient id="rrpFill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#60a5fa" stopOpacity={0.2} />
                    <stop offset="100%" stopColor="#60a5fa" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="2 4" stroke="#1e293b" vertical={false} />
                <XAxis
                  dataKey="ts"
                  tickFormatter={ts => fmtAxisForPeriod(ts, period)}
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={{ stroke: '#1e293b' }}
                  tickLine={false}
                  minTickGap={56}
                />
                <YAxis
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={v => `$${v}`}
                  width={52}
                />
                <Tooltip content={<PriceTooltip />} />
                <ReferenceLine y={0} stroke="#f87171" strokeDasharray="4 2" strokeOpacity={0.4} />
                <Area
                  type="stepAfter"
                  dataKey="rrp"
                  stroke="#60a5fa"
                  strokeWidth={1.5}
                  fill="url(#rrpFill)"
                  dot={false}
                  activeDot={{ r: 3, fill: '#60a5fa', stroke: '#0f172a', strokeWidth: 2 }}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      <div className="mb-4">
        <BackfillPanel
          latestDate={meta?.max_date}
          onSync={async (startDateStr, endDateStr) => {
            const r = await pricesApi.sync(startDateStr, endDateStr)
            const m = await pricesApi.getMeta()
            setMeta(m)
            fetchChart()
            return r
          }}
        />
      </div>

      {hasData && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
          <div className="px-6 py-3.5 border-b border-slate-800">
            <h2 className="text-sm font-medium text-slate-300">Recent Intervals</h2>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-800">
                <th className="text-left px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">Time (AEST)</th>
                <th className="text-right px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">RRP (A$/MWh)</th>
              </tr>
            </thead>
            <tbody>
              {[...chartData].reverse().slice(0, 100).map((row, i) => (
                <tr key={i} className="border-b border-slate-800/40 hover:bg-slate-800/20 transition-colors">
                  <td className="px-6 py-2 text-slate-400 font-mono text-xs">{fmtFull(row.ts)}</td>
                  <td className={`px-6 py-2 text-right font-mono tabular-nums text-xs ${row.rrp < 0 ? 'text-red-400' : 'text-slate-200'}`}>
                    ${row.rrp?.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
