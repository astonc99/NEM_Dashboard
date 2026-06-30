import { useEffect, useState, useCallback } from 'react'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { generation as genApi } from '../api/client'
import LoadingSpinner from '../components/LoadingSpinner'
import EmptyState from '../components/EmptyState'
import BackfillPanel from '../components/BackfillPanel'
import TimeRangeButtons, { activePeriodLabel, fmtAxisForPeriod } from '../components/TimeRangeButtons'
import { useSyncStatus } from '../context/SyncContext'

function fmtFull(ts) {
  return new Date(ts).toLocaleString('en-AU', {
    day: '2-digit', month: 'short',
    hour: '2-digit', minute: '2-digit',
  })
}

function GenTooltip({ active, payload, label, fuelColors }) {
  if (!active || !payload?.length) return null
  const total = payload.reduce((s, p) => s + (p.value || 0), 0)
  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2.5 shadow-xl text-xs min-w-40">
      <p className="text-slate-400 mb-2">{fmtFull(label)}</p>
      {[...payload].reverse().filter(p => p.value > 0).map(p => (
        <div key={p.dataKey} className="flex items-center justify-between gap-6 mb-0.5">
          <span className="flex items-center gap-1.5">
            <span
              className="w-2 h-2 rounded-sm flex-shrink-0"
              style={{ background: fuelColors?.[p.dataKey] ?? p.fill }}
            />
            <span className="text-slate-300">{p.dataKey}</span>
          </span>
          <span className="text-slate-100 tabular-nums font-medium">{p.value?.toFixed(0)} MW</span>
        </div>
      ))}
      <div className="border-t border-slate-700 mt-1.5 pt-1.5 flex items-center justify-between">
        <span className="text-slate-400">Total</span>
        <span className="text-slate-100 font-semibold tabular-nums">{total.toFixed(0)} MW</span>
      </div>
    </div>
  )
}

export default function GenerationMix() {
  const { refreshKey, status: syncStatus } = useSyncStatus()
  const [meta, setMeta] = useState(null)
  const [genData, setGenData] = useState(null)
  const [topUnits, setTopUnits] = useState(null)
  const [loading, setLoading] = useState(false)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')

  useEffect(() => {
    genApi.getMeta().then(m => {
      setMeta(m)
      if (m.max_date) {
        // Default to the full month that contains the latest available data
        const end = new Date(m.max_date)
        const start = new Date(end.getFullYear(), end.getMonth(), 1)
        setStartDate(start.toISOString().slice(0, 10))
        setEndDate(m.max_date)
      }
    }).catch(() => {})
  }, [refreshKey])

  const fetchData = useCallback(() => {
    if (!startDate || !endDate) return
    setLoading(true)
    Promise.all([
      genApi.getData({ start_date: startDate, end_date: endDate }),
      genApi.getTopUnits({ start_date: startDate, end_date: endDate }),
    ])
      .then(([gen, top]) => {
        setGenData(gen)
        setTopUnits(top.data)
      })
      .catch(() => {
        setGenData({ data: [], fuels: [], fuel_colors: {} })
        setTopUnits([])
      })
      .finally(() => setLoading(false))
  }, [startDate, endDate])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  const hasData = genData?.data?.length > 0
  const period  = activePeriodLabel(startDate, endDate)

  return (
    <div className="p-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-100">Generation Mix</h1>
        <p className="text-slate-500 text-sm mt-1">NEM Victoria · SCADA unit dispatch · Fuel-type stacking</p>
      </div>

      {meta && (
        <div className="grid grid-cols-3 gap-3 mb-6">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">VIC Units</p>
            <p className="text-2xl font-bold text-slate-100">{meta.duid_count}</p>
            <p className="text-xs text-slate-600 mt-1">Registered DUIDs</p>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">Data Start</p>
            <p className="text-xl font-bold text-slate-100">{meta.min_date ?? '—'}</p>
            <p className="text-xs text-slate-600 mt-1">Earliest SCADA record</p>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">Data End</p>
            <p className="text-xl font-bold text-slate-100">{meta.max_date ?? '—'}</p>
            <p className="text-xs text-slate-600 mt-1">Latest SCADA record</p>
          </div>
        </div>
      )}

      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 mb-4">
        <div className="flex items-start justify-between mb-5 gap-4">
          <div className="flex-shrink-0">
            <h2 className="text-sm font-medium text-slate-200">Generation Stack by Fuel</h2>
            {hasData && (
              <p className="text-xs text-slate-600 mt-0.5">
                {genData.data.length.toLocaleString()} intervals · {genData.fuels.length} fuel types
              </p>
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
                className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-blue-500/60 transition-colors"
              />
              <span className="text-slate-700 text-xs">to</span>
              <input
                type="date"
                value={endDate}
                min={startDate || (meta?.min_date ?? '')}
                max={meta?.max_date ?? ''}
                onChange={e => setEndDate(e.target.value)}
                className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-blue-500/60 transition-colors"
              />
            </div>
          </div>
        </div>

        {loading ? (
          <LoadingSpinner message="Loading generation data..." />
        ) : !hasData && syncStatus === 'running' ? (
          <LoadingSpinner message="Syncing SCADA data in background — this may take a few minutes…" />
        ) : !hasData ? (
          <EmptyState
            title="No generation data for this range"
            description="Use the backfill panel below to download SCADA data for this month, then refresh."
          />
        ) : (
          <div className="h-96">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={genData.data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
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
                  tickFormatter={v => `${v} MW`}
                  width={64}
                />
                <Tooltip content={<GenTooltip fuelColors={genData.fuel_colors} />} />
                <Legend
                  wrapperStyle={{ fontSize: '11px', paddingTop: '14px' }}
                  formatter={v => <span style={{ color: '#94a3b8' }}>{v}</span>}
                />
                {genData.fuels.map(fuel => (
                  <Area
                    key={fuel}
                    type="stepAfter"
                    dataKey={fuel}
                    stackId="stack"
                    stroke={genData.fuel_colors[fuel]}
                    fill={genData.fuel_colors[fuel]}
                    fillOpacity={0.85}
                    strokeWidth={0}
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      <div className="mb-4">
        <BackfillPanel
          latestDate={meta?.max_date}
          onSync={async (startDateStr, endDateStr) => {
            const r = await genApi.sync(startDateStr, endDateStr)
            const m = await genApi.getMeta()
            setMeta(m)
            fetchData()
            return r
          }}
        />
      </div>

      {topUnits && topUnits.length > 0 && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
          <div className="px-6 py-3.5 border-b border-slate-800">
            <h2 className="text-sm font-medium text-slate-300">Top Generation Units</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-800">
                  <th className="text-left px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">DUID</th>
                  <th className="text-left px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">Station</th>
                  <th className="text-left px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">Fuel</th>
                  <th className="text-right px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">Avg MW</th>
                  <th className="text-right px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">Energy MWh</th>
                  <th className="text-right px-6 py-2.5 text-xs text-slate-500 font-medium uppercase tracking-wider">Intervals</th>
                </tr>
              </thead>
              <tbody>
                {topUnits.map((unit, i) => (
                  <tr key={i} className="border-b border-slate-800/40 hover:bg-slate-800/20 transition-colors">
                    <td className="px-6 py-2 font-mono text-xs text-slate-300">{unit.DUID}</td>
                    <td className="px-6 py-2 text-slate-400 text-xs">{unit.Station}</td>
                    <td className="px-6 py-2 text-xs text-slate-500">{unit.Fuel}</td>
                    <td className="px-6 py-2 text-right font-mono tabular-nums text-slate-200 text-xs">
                      {unit.avg_mw?.toFixed(1)}
                    </td>
                    <td className="px-6 py-2 text-right font-mono tabular-nums text-slate-400 text-xs">
                      {unit.energy_mwh?.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </td>
                    <td className="px-6 py-2 text-right font-mono tabular-nums text-slate-600 text-xs">
                      {unit.intervals?.toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
