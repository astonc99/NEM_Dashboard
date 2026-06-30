import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import DemoBanner from './DemoBanner'

const DEMO = import.meta.env.VITE_DEMO_MODE === 'true'

export default function Layout() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-slate-950">
      {DEMO && <DemoBanner />}
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
