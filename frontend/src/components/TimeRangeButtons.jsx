/**
 * Quick-select time range buttons for chart date filters.
 *
 * NEM context for each period:
 *   1D  — Single dispatch day (288 × 5-min intervals). Intraday price/generation patterns.
 *   7D  — ST PASA horizon. AEMO's Short-Term Planning and Security Assessment uses a 7-day window.
 *   1M  — Monthly. Standard AEMO reporting period (Quarterly Energy Dynamics, MCC, etc.).
 *   3M  — Quarterly. Aligns with NEM financial quarters (Jul-Sep, Oct-Dec, Jan-Mar, Apr-Jun).
 *   1Y  — Annual. Rolling 12-month window, aligns with NEM financial year (July–June).
 *   All — Full stored dataset.
 */

const PERIODS = [
  { label: '1D',  title: 'Dispatch Day',        days: 1   },
  { label: '7D',  title: 'ST PASA Horizon',     days: 7   },
  { label: '1M',  title: 'Monthly',             days: 30  },
  { label: '3M',  title: 'Quarterly',           days: 91  },
  { label: '1Y',  title: 'Financial Year',      days: 365 },
  { label: 'All', title: 'Full Dataset',        days: null},
]

/**
 * Determine which period label best matches the current startDate / endDate.
 * Returns the label string, or null if no period matches closely.
 */
export function activePeriodLabel(startDate, endDate) {
  if (!startDate || !endDate) return null
  const end   = new Date(endDate)
  const start = new Date(startDate)
  const diff  = Math.round((end - start) / (1000 * 60 * 60 * 24))
  if (diff <= 1)   return '1D'
  if (diff <= 8)   return '7D'
  if (diff <= 32)  return '1M'
  if (diff <= 95)  return '3M'
  if (diff <= 370) return '1Y'
  return 'All'
}

/**
 * Format axis tick label depending on active period.
 * Short periods → show time (HH:MM); longer → show date (DD Mon).
 */
export function fmtAxisForPeriod(ts, period) {
  const d = new Date(ts)
  if (period === '1D') {
    return d.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit', hour12: false })
  }
  if (period === '7D') {
    return d.toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric' })
  }
  return d.toLocaleDateString('en-AU', { day: '2-digit', month: 'short' })
}

export default function TimeRangeButtons({ maxDate, minDate, startDate, endDate, onSelect }) {
  const active = activePeriodLabel(startDate, endDate)

  function select(days) {
    if (!maxDate) return
    const end   = new Date(maxDate)
    let start
    if (days === null) {
      start = new Date(minDate || '2009-01-01')
    } else {
      start = new Date(end)
      start.setDate(start.getDate() - (days - 1))
      if (minDate && start < new Date(minDate)) {
        start = new Date(minDate)
      }
    }
    onSelect(start.toISOString().slice(0, 10), maxDate)
  }

  return (
    <div className="flex items-center gap-1">
      {PERIODS.map(({ label, title, days }) => (
        <button
          key={label}
          onClick={() => select(days)}
          title={title}
          disabled={!maxDate}
          className={`px-2.5 py-1 text-xs rounded font-medium transition-colors disabled:opacity-40 ${
            active === label
              ? 'bg-blue-500/20 text-blue-400 border border-blue-500/40'
              : 'text-slate-500 hover:text-slate-300 hover:bg-slate-800 border border-transparent'
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  )
}
