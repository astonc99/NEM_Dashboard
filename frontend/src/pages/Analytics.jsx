import { useEffect, useState, useCallback } from 'react'
import {
  ComposedChart, AreaChart, LineChart,
  Area, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ReferenceLine, Legend, ResponsiveContainer,
} from 'recharts'
import { analytics as analyticsApi, generation as genApi, prices as pricesApi } from '../api/client'
import LoadingSpinner from '../components/LoadingSpinner'
import EmptyState from '../components/EmptyState'
import TimeRangeButtons from '../components/TimeRangeButtons'

function fmtAxis(ts) {
  return new Date(ts).toLocaleDateString('en-AU', { day: '2-digit', month: 'short' })
}

function StatCard({ label, value, sub, color = 'text-slate-100' }) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
      <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">{label}</p>
      <p className={`text-2xl font-bold ${color}`}>{value ?? '—'}</p>
      {sub && <p className="text-xs text-slate-600 mt-1">{sub}</p>}
    </div>
  )
}

function RenewableTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 shadow-xl text-xs min-w-44">
      <p className="text-slate-400 mb-2">{fmtAxis(label)}</p>
      <div className="space-y-0.5">
        <div className="flex justify-between gap-6">
          <span className="flex items-center gap-1.5">
            <span className="w-2 h-2 rounded-sm bg-emerald-500" />
            <span className="text-slate-300">Renewable</span>
          </span>
          <span className="text-slate-100 tabular-nums">{d?.renewable_mw?.toFixed(0)} MW</span>
        </div>
        <div className="flex justify-between gap-6">
          <span className="flex items-center gap-1.5">
            <span className="w-2 h-2 rounded-sm bg-slate-600" />
            <span className="text-slate-300">Other</span>
          </span>
          <span className="text-slate-100 tabular-nums">{d?.other_mw?.toFixed(0)} MW</span>
        </div>
      </div>
      <div className="border-t border-slate-700 mt-1.5 pt-1.5 flex justify-between">
        <span className="text-slate-400">Penetration</span>
        <span className="text-emerald-400 font-semibold">{d?.pct?.toFixed(1)}%</span>
      </div>
    </div>
  )
}

function DurationTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const { pct_time, rrp } = payload[0].payload
  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 shadow-xl text-xs">
      <p className="text-slate-400 mb-1">{pct_time?.toFixed(1)}% of time</p>
      <p className={`font-semibold tabular-nums ${rrp < 0 ? 'text-red-400' : 'text-blue-300'}`}>
        ${rrp?.toFixed(2)} /MWh
      </p>
    </div>
  )
}

export default function Analytics() {
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate]     = useState('')
  const [genMeta, setGenMeta]     = useState(null)
  const [priceMeta, setPriceMeta] = useState(null)

  const [renData, setRenData]   = useState(null)
  const [renLoading, setRenLoading] = useState(false)

  const [durData, setDurData]   = useState(null)
  const [durLoading, setDurLoading] = useState(false)

  // Load metadata to determine available date ranges
  useEffect(() => {
    Promise.all([genApi.getMeta(), pricesApi.getMeta()])
      .then(([gm, pm]) => {
        setGenMeta(gm)
        setPriceMeta(pm)
        // Use the intersection of available ranges as the default
        const start = gm.min_date ?? pm.min_date
        const end   = gm.max_date ?? pm.max_date
        if (start && end) {
          // Default to the last full month of the most limiting dataset
          const endDate = new Date(end)
          const startDate = new Date(endDate.getFullYear(), endDate.getMonth(), 1)
          setStartDate(startDate.toISOString().slice(0, 10))
          setEndDate(end)
        }
      })
      .catch(() => {})
  }, [])

  const fetchRenewable = useCallback(() => {
    if (!startDate || !endDate) return
    setRenLoading(true)
    analyticsApi
      .getRenewablePenetration({ start_date: startDate, end_date: endDate })
      .then(setRenData)
      .catch(() => setRenData({ data: [], avg_pct: null }))
      .finally(() => setRenLoading(false))
  }, [startDate, endDate])

  const fetchDuration = useCallback(() => {
    if (!startDate || !endDate) return
    setDurLoading(true)
    analyticsApi
      .getPriceDuration({ start_date: startDate, end_date: endDate })
      .then(setDurData)
      .catch(() => setDurData({ data: [], pct_negative: null, median_rrp: null }))
      .finally(() => setDurLoading(false))
  }, [startDate, endDate])

  useEffect(() => { fetchRenewable() }, [fetchRenewable])
  useEffect(() => { fetchDuration() },  [fetchDuration])

  const minDate = genMeta?.min_date ?? priceMeta?.min_date ?? ''
  const maxDate = genMeta?.max_date ?? priceMeta?.max_date ?? ''

  return (
    <div className="p-8">
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Analytics</h1>
          <p className="text-slate-500 text-sm mt-1">NEM Victoria · Renewable penetration & price duration</p>
        </div>
        <div className="flex flex-col items-end gap-2">
          <TimeRangeButtons
            maxDate={maxDate}
            minDate={minDate}
            startDate={startDate}
            endDate={endDate}
            onSelect={(s, e) => { setStartDate(s); setEndDate(e) }}
          />
          <div className="flex items-center gap-2">
            <input
              type="date"
              value={startDate}
              min={minDate}
              max={endDate || maxDate}
              onChange={e => setStartDate(e.target.value)}
              className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-blue-500/60"
            />
            <span className="text-slate-700 text-xs">to</span>
            <input
              type="date"
              value={endDate}
              min={startDate || minDate}
              max={maxDate}
              onChange={e => setEndDate(e.target.value)}
              className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-blue-500/60"
            />
          </div>
        </div>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-4 gap-3 mb-6">
        <StatCard
          label="Avg Renewable %"
          value={renData?.avg_pct != null ? `${renData.avg_pct}%` : null}
          sub="Wind + Solar + Hydro + Bioenergy"
          color={renData?.avg_pct >= 50 ? 'text-emerald-400' : 'text-slate-100'}
        />
        <StatCard
          label="% Time Negative Price"
          value={durData?.pct_negative != null ? `${durData.pct_negative}%` : null}
          sub="Intervals with RRP < $0"
          color={durData?.pct_negative > 10 ? 'text-red-400' : 'text-slate-100'}
        />
        <StatCard
          label="Median Price"
          value={durData?.median_rrp != null ? `$${durData.median_rrp}` : null}
          sub="A$/MWh · 50th percentile"
        />
        <StatCard
          label="Total Intervals"
          value={durData?.total_intervals?.toLocaleString() ?? null}
          sub="5-minute dispatch intervals"
        />
      </div>

      {/* Renewable Penetration */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 mb-4">
        <div className="mb-4">
          <h2 className="text-sm font-medium text-slate-200">Renewable Penetration</h2>
          <p className="text-xs text-slate-600 mt-0.5">
            Wind, Solar, Hydro & Bioenergy as share of VIC1 dispatch · 30-min aggregates
          </p>
        </div>

        {renLoading ? (
          <LoadingSpinner message="Loading renewable data..." />
        ) : !renData?.data?.length ? (
          <EmptyState title="No generation data" description="Backfill SCADA data from the Generation Mix page first." />
        ) : (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={renData.data} margin={{ top: 4, right: 48, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="2 4" stroke="#1e293b" vertical={false} />
                <XAxis
                  dataKey="ts"
                  tickFormatter={fmtAxis}
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={{ stroke: '#1e293b' }}
                  tickLine={false}
                  minTickGap={60}
                />
                <YAxis
                  yAxisId="mw"
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={v => `${v} MW`}
                  width={68}
                />
                <YAxis
                  yAxisId="pct"
                  orientation="right"
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={v => `${v}%`}
                  domain={[0, 100]}
                  width={40}
                />
                <Tooltip content={<RenewableTooltip />} />
                <Area
                  yAxisId="mw"
                  type="monotone"
                  dataKey="renewable_mw"
                  stackId="gen"
                  stroke="#10b981"
                  fill="#10b981"
                  fillOpacity={0.7}
                  strokeWidth={0}
                  dot={false}
                  isAnimationActive={false}
                  name="Renewable MW"
                />
                <Area
                  yAxisId="mw"
                  type="monotone"
                  dataKey="other_mw"
                  stackId="gen"
                  stroke="#334155"
                  fill="#334155"
                  fillOpacity={0.6}
                  strokeWidth={0}
                  dot={false}
                  isAnimationActive={false}
                  name="Other MW"
                />
                <Line
                  yAxisId="pct"
                  type="monotone"
                  dataKey="pct"
                  stroke="#fbbf24"
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                  name="Renewable %"
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Price Duration Curve */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6">
        <div className="mb-4">
          <h2 className="text-sm font-medium text-slate-200">Price Duration Curve</h2>
          <p className="text-xs text-slate-600 mt-0.5">
            Dispatch price sorted from highest to lowest — shows how often VIC1 trades at each price level
          </p>
        </div>

        {durLoading ? (
          <LoadingSpinner message="Loading price data..." />
        ) : !durData?.data?.length ? (
          <EmptyState title="No price data" description="Backfill price data from the Spot Prices page first." />
        ) : (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={durData.data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="2 4" stroke="#1e293b" vertical={false} />
                <XAxis
                  dataKey="pct_time"
                  type="number"
                  domain={[0, 100]}
                  tickFormatter={v => `${v}%`}
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={{ stroke: '#1e293b' }}
                  tickLine={false}
                  label={{ value: '% of time exceeded', position: 'insideBottom', offset: -4, fill: '#475569', fontSize: 11 }}
                />
                <YAxis
                  tick={{ fill: '#475569', fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={v => `$${v}`}
                  width={60}
                />
                <Tooltip content={<DurationTooltip />} />
                <ReferenceLine y={0}    stroke="#f87171" strokeDasharray="4 2" strokeOpacity={0.5} label={{ value: '$0', fill: '#f87171', fontSize: 10, position: 'right' }} />
                <ReferenceLine y={300}  stroke="#fb923c" strokeDasharray="4 2" strokeOpacity={0.4} label={{ value: '$300', fill: '#fb923c', fontSize: 10, position: 'right' }} />
                <ReferenceLine y={5000} stroke="#ef4444" strokeDasharray="4 2" strokeOpacity={0.3} label={{ value: '$5000', fill: '#ef4444', fontSize: 10, position: 'right' }} />
                <Line
                  type="monotone"
                  dataKey="rrp"
                  stroke="#60a5fa"
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  )
}
