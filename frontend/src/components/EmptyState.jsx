export default function EmptyState({ title, description }) {
  return (
    <div className="flex flex-col items-center justify-center py-24 gap-2 text-center">
      <div className="w-11 h-11 rounded-xl bg-slate-800 flex items-center justify-center mb-2">
        <svg className="w-5 h-5 text-slate-600" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 13.5h3.86a2.25 2.25 0 0 1 2.012 1.244l.256.512a2.25 2.25 0 0 0 2.013 1.244h3.218a2.25 2.25 0 0 0 2.013-1.244l.256-.512a2.25 2.25 0 0 1 2.013-1.244h3.859m-19.5.338V18a2.25 2.25 0 0 0 2.25 2.25h15A2.25 2.25 0 0 0 21.75 18v-4.162c0-.224-.034-.447-.1-.661L19.24 5.338a2.25 2.25 0 0 0-2.15-1.588H6.911a2.25 2.25 0 0 0-2.15 1.588L2.35 13.177a2.25 2.25 0 0 0-.1.661Z" />
        </svg>
      </div>
      <p className="text-slate-300 font-medium text-sm">{title}</p>
      <p className="text-xs text-slate-600 max-w-xs leading-relaxed">{description}</p>
    </div>
  )
}
