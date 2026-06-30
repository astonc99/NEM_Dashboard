export default function MetricCard({ label, value, sub, trend, loading }) {
  const trendClass =
    trend === 'negative' ? 'text-red-400' :
    trend === 'positive' ? 'text-emerald-400' :
    'text-slate-500'

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
      <p className="text-xs text-slate-500 uppercase tracking-wider mb-2 font-medium">{label}</p>
      {loading ? (
        <div className="h-8 w-28 bg-slate-800 animate-pulse rounded-md" />
      ) : (
        <p className="text-2xl font-semibold text-slate-100 tabular-nums">{value ?? '—'}</p>
      )}
      {sub && (
        <p className={`text-xs mt-1.5 ${trendClass}`}>{sub}</p>
      )}
    </div>
  )
}
