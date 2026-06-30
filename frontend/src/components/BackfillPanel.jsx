import { useState } from 'react'

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

function monthsInRange(sy, sm, ey, em) {
  return (ey - sy) * 12 + (em - sm) + 1
}

export default function BackfillPanel({ onSync, latestDate }) {
  const now = new Date()
  const currentYear  = now.getFullYear()
  const currentMonth = now.getMonth() + 1

  // Default: 12 months prior → current month
  const twelveMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 11, 1)
  const [startYear,  setStartYear]  = useState(twelveMonthsAgo.getFullYear())
  const [startMonth, setStartMonth] = useState(twelveMonthsAgo.getMonth() + 1)
  const [endYear,    setEndYear]    = useState(currentYear)
  const [endMonth,   setEndMonth]   = useState(currentMonth)

  const [status, setStatus] = useState(null)  // null | 'loading' | 'done' | 'error'
  const [result, setResult] = useState(null)

  const numMonths = monthsInRange(startYear, startMonth, endYear, endMonth)
  const validRange = numMonths > 0 && numMonths <= 60

  const startDateStr = `${startYear}-${String(startMonth).padStart(2, '0')}-01`
  const endDateStr   = `${endYear}-${String(endMonth).padStart(2, '0')}-01`

  async function run() {
    if (!validRange) return
    setStatus('loading')
    setResult(null)
    try {
      const r = await onSync(startDateStr, endDateStr)
      setStatus('done')
      setResult(r)
    } catch (err) {
      setStatus('error')
      setResult({ error: err?.response?.data?.detail ?? err.message ?? 'Failed' })
    }
  }

  const yearOptions = Array.from({ length: currentYear - 2009 + 1 }, (_, i) => 2009 + i).reverse()

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
      <p className="text-xs font-medium text-slate-400 uppercase tracking-wider mb-3">
        Backfill Range
        {latestDate && (
          <span className="ml-2 normal-case font-normal text-slate-600">
            · latest stored: <span className="text-slate-400">{latestDate}</span>
          </span>
        )}
      </p>

      <div className="flex items-center gap-2 flex-wrap">
        {/* Start */}
        <span className="text-xs text-slate-500">From</span>
        <select
          value={startMonth}
          onChange={e => setStartMonth(Number(e.target.value))}
          className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-2 py-1.5 focus:outline-none focus:border-blue-500/60"
        >
          {MONTHS.map((mo, i) => <option key={mo} value={i + 1}>{mo}</option>)}
        </select>
        <select
          value={startYear}
          onChange={e => setStartYear(Number(e.target.value))}
          className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-2 py-1.5 focus:outline-none focus:border-blue-500/60"
        >
          {yearOptions.map(y => <option key={y} value={y}>{y}</option>)}
        </select>

        <span className="text-xs text-slate-600">→</span>

        {/* End */}
        <span className="text-xs text-slate-500">To</span>
        <select
          value={endMonth}
          onChange={e => setEndMonth(Number(e.target.value))}
          className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-2 py-1.5 focus:outline-none focus:border-blue-500/60"
        >
          {MONTHS.map((mo, i) => <option key={mo} value={i + 1}>{mo}</option>)}
        </select>
        <select
          value={endYear}
          onChange={e => setEndYear(Number(e.target.value))}
          className="bg-slate-800 border border-slate-700 text-slate-300 text-xs rounded-lg px-2 py-1.5 focus:outline-none focus:border-blue-500/60"
        >
          {yearOptions.map(y => <option key={y} value={y}>{y}</option>)}
        </select>

        {/* Count hint */}
        {validRange && (
          <span className="text-xs text-slate-600">{numMonths} month{numMonths !== 1 ? 's' : ''}</span>
        )}
        {!validRange && numMonths <= 0 && (
          <span className="text-xs text-red-400">End must be after start</span>
        )}

        <button
          onClick={run}
          disabled={status === 'loading' || !validRange}
          className="flex items-center gap-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs font-medium px-3 py-1.5 rounded-lg transition-colors"
        >
          {status === 'loading' ? (
            <>
              <svg className="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Downloading…
            </>
          ) : 'Backfill'}
        </button>

        {/* Status */}
        {status === 'done' && (
          <span className="text-xs text-emerald-400">
            {result?.months_synced?.length
              ? `Done — ${result.months_synced.length} month(s) updated`
              : 'Already up to date'}
            {result?.more_available && ' · more available, run again'}
          </span>
        )}
        {status === 'error' && (
          <span className="text-xs text-red-400">{result?.error ?? 'Error'}</span>
        )}
        {result?.errors?.length > 0 && status === 'done' && (
          <span className="text-xs text-amber-400">
            {result.errors.length} month(s) had errors — check console
          </span>
        )}
      </div>
    </div>
  )
}
