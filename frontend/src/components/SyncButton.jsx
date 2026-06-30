import { useState } from 'react'

export default function SyncButton({ onSync, latestDate, label = 'Sync to Today' }) {
  const [status, setStatus] = useState(null)  // null | 'syncing' | 'done' | 'error'
  const [result, setResult] = useState(null)

  async function run() {
    setStatus('syncing')
    setResult(null)
    try {
      const r = await onSync()
      setStatus(r.months_synced?.length > 0 ? 'done' : 'up_to_date')
      setResult(r)
    } catch (err) {
      setStatus('error')
      setResult({ error: err?.response?.data?.detail ?? err.message ?? 'Sync failed' })
    }
  }

  const statusColors = {
    syncing:    'text-blue-400',
    done:       'text-emerald-400',
    up_to_date: 'text-slate-400',
    error:      'text-red-400',
  }

  const statusText = {
    syncing:    'Syncing…',
    done:       `Updated — ${result?.months_synced?.length ?? 0} month(s) added`,
    up_to_date: 'Already up to date',
    error:      result?.error ?? 'Error',
  }

  return (
    <div className="flex items-center gap-3 bg-slate-900 border border-slate-800 rounded-xl px-4 py-3">
      <div className="flex-1 min-w-0">
        <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Data</p>
        <p className="text-xs text-slate-500 mt-0.5 truncate">
          Latest: <span className="text-slate-300">{latestDate ?? 'No data'}</span>
        </p>
      </div>

      <div className="flex items-center gap-2">
        {status && (
          <span className={`text-xs ${statusColors[status]}`}>
            {statusText[status]}
          </span>
        )}
        {result?.more_available && (
          <span className="text-xs text-amber-400">More available — sync again</span>
        )}
        <button
          onClick={run}
          disabled={status === 'syncing'}
          className="flex items-center gap-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs font-medium px-3 py-1.5 rounded-lg transition-colors"
        >
          {status === 'syncing' ? (
            <>
              <svg className="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Syncing
            </>
          ) : (
            <>
              <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
              </svg>
              {label}
            </>
          )}
        </button>
      </div>
    </div>
  )
}
