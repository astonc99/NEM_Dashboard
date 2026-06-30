import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { prices as pricesApi, generation as genApi } from '../api/client'

export default function Home() {
  const [priceMeta, setPriceMeta] = useState(null)
  const [genMeta, setGenMeta] = useState(null)
  const [summary, setSummary] = useState(null)

  useEffect(() => {
    pricesApi.getMeta().then(setPriceMeta).catch(() => {})
    pricesApi.getSummary().then(setSummary).catch(() => {})
    genApi.getMeta().then(setGenMeta).catch(() => {})
  }, [])

  return (
    <div className="p-8 max-w-4xl mx-auto">
      <div className="mb-10">
        <p className="text-xs font-medium text-blue-400 uppercase tracking-widest mb-2">National Electricity Market</p>
        <h1 className="text-3xl font-bold text-slate-100">Victoria Dashboard</h1>
        <p className="text-slate-500 mt-2 text-sm">
          Historical dispatch prices and generation mix for the VIC1 region · Data from AEMO MMSDM
        </p>
      </div>

      {summary && (
        <div className="grid grid-cols-3 gap-3 mb-10">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">Latest RRP</p>
            <p className={`text-2xl font-bold tabular-nums ${summary.latest_rrp < 0 ? 'text-red-400' : 'text-slate-100'}`}>
              ${summary.latest_rrp?.toFixed(2)}
            </p>
            <p className="text-xs text-slate-600 mt-1">A$/MWh · VIC1</p>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">Day Average</p>
            <p className={`text-2xl font-bold tabular-nums ${summary.today_avg_rrp < 0 ? 'text-red-400' : 'text-slate-100'}`}>
              ${summary.today_avg_rrp?.toFixed(2)}
            </p>
            <p className="text-xs text-slate-600 mt-1">{summary.latest_date}</p>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-1.5 font-medium">Negative Intervals</p>
            <p className={`text-2xl font-bold tabular-nums ${summary.negative_intervals_today > 0 ? 'text-amber-400' : 'text-slate-100'}`}>
              {summary.negative_intervals_today}
            </p>
            <p className="text-xs text-slate-600 mt-1">Today · RRP &lt; $0</p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-3 gap-4">
        <Link
          to="/prices"
          className="group bg-slate-900 border border-slate-800 hover:border-blue-500/40 rounded-xl p-6 transition-all duration-200"
        >
          <div className="w-9 h-9 rounded-lg bg-blue-500/10 border border-blue-500/20 flex items-center justify-center mb-4 group-hover:bg-blue-500/20 transition-colors">
            <svg className="w-4.5 h-4.5 text-blue-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18 9 11.25l4.306 4.306a11.95 11.95 0 0 1 5.814-5.518l2.74-1.22m0 0-5.94-2.281m5.94 2.28-2.28 5.941" />
            </svg>
          </div>
          <h2 className="text-base font-semibold text-slate-100 mb-1.5">Spot Prices</h2>
          <p className="text-sm text-slate-500 leading-relaxed">
            5-minute RRP from AEMO dispatch · Interactive time-series chart and interval table
          </p>
          {priceMeta?.count > 0 && (
            <div className="mt-4 pt-4 border-t border-slate-800 flex items-center justify-between">
              <span className="text-xs text-slate-600">{priceMeta.count?.toLocaleString()} intervals</span>
              <span className="text-xs text-slate-600">{priceMeta.min_date} → {priceMeta.max_date}</span>
            </div>
          )}
        </Link>

        <Link
          to="/generation"
          className="group bg-slate-900 border border-slate-800 hover:border-cyan-500/40 rounded-xl p-6 transition-all duration-200"
        >
          <div className="w-9 h-9 rounded-lg bg-cyan-500/10 border border-cyan-500/20 flex items-center justify-center mb-4 group-hover:bg-cyan-500/20 transition-colors">
            <svg className="w-4.5 h-4.5 text-cyan-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 3v11.25A2.25 2.25 0 0 0 6 16.5h2.25M3.75 3h-1.5m1.5 0h16.5m0 0h1.5m-1.5 0v11.25A2.25 2.25 0 0 1 18 16.5h-2.25m-7.5 0h7.5m-7.5 0-1 3m8.5-3 1 3m0 0 .5 1.5m-.5-1.5h-9.5m0 0-.5 1.5M9 11.25v1.5M12 9v3.75m3-6v6" />
            </svg>
          </div>
          <h2 className="text-base font-semibold text-slate-100 mb-1.5">Generation Mix</h2>
          <p className="text-sm text-slate-500 leading-relaxed">
            SCADA unit dispatch by fuel type · Stacked area chart with top generators table
          </p>
          {genMeta?.duid_count > 0 && (
            <div className="mt-4 pt-4 border-t border-slate-800 flex items-center justify-between">
              <span className="text-xs text-slate-600">{genMeta.duid_count} VIC units</span>
              <span className="text-xs text-slate-600">{genMeta.min_date} → {genMeta.max_date}</span>
            </div>
          )}
        </Link>

        <Link
          to="/analytics"
          className="group bg-slate-900 border border-slate-800 hover:border-emerald-500/40 rounded-xl p-6 transition-all duration-200"
        >
          <div className="w-9 h-9 rounded-lg bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center mb-4 group-hover:bg-emerald-500/20 transition-colors">
            <svg className="w-4.5 h-4.5 text-emerald-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M7.5 14.25v2.25m3-4.5v4.5m3-6.75v6.75m3-9v9M6 20.25h12A2.25 2.25 0 0 0 20.25 18V6A2.25 2.25 0 0 0 18 3.75H6A2.25 2.25 0 0 0 3.75 6v12A2.25 2.25 0 0 0 6 20.25Z" />
            </svg>
          </div>
          <h2 className="text-base font-semibold text-slate-100 mb-1.5">Analytics</h2>
          <p className="text-sm text-slate-500 leading-relaxed">
            Renewable penetration curve and price duration analysis for the VIC1 region
          </p>
        </Link>
      </div>
    </div>
  )
}
