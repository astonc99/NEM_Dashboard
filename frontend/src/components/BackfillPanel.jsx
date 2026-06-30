import { useState } from 'react'

const DEMO = import.meta.env.VITE_DEMO_MODE === 'true'

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

function monthsInRange(sy, sm, ey, em) {
  return (ey - sy) * 12 + (em - sm) + 1
}

export default function BackfillPanel({ onSync, latestDate }) {
  if (DEMO) {
    return (
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 flex items-center justify-between gap-4">
        <div>
          <p className="text-xs font-medium text-slate-400 uppercase tracking-wider mb-0.5">Demo Mode</p>
          <p className="text-xs text-slate-500">
            This demo shows <span className="text-slate-300">Jan – Feb 2026</span> data only.
            Clone the repo to run locally with full dataset access and live AEMO sync.
          </p>
        </div>
        <a
          href="https://github.com/astonc99/NEM_Dashboard"
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1.5 bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 text-xs font-medium px-3 py-1.5 rounded-lg transition-colors flex-shrink-0"
        >
          <svg viewBox="0 0 24 24" fill="currentColor" className="w-3.5 h-3.5">
            <path d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" />
          </svg>
          View on GitHub
        </a>
      </div>
    )
  }

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
