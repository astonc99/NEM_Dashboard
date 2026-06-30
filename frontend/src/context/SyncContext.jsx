import { createContext, useContext, useEffect, useRef, useState } from 'react'
import axios from 'axios'

const SyncContext = createContext({ status: 'idle', refreshKey: 0 })

const DEMO = import.meta.env.VITE_DEMO_MODE === 'true'
const POLL_MS = 5000

export function SyncProvider({ children }) {
  const [status, setStatus] = useState('idle')
  const [refreshKey, setRefreshKey] = useState(0)
  const prevStatus = useRef('idle')

  useEffect(() => {
    if (DEMO) return

    const poll = async () => {
      try {
        const { data } = await axios.get(
          (import.meta.env.VITE_API_URL ?? '/api') + '/sync/status'
        )
        const s = data.status ?? 'idle'
        setStatus(s)

        // Trigger a meta refresh on all pages the moment sync finishes
        if (prevStatus.current === 'running' && (s === 'done' || s === 'error')) {
          setRefreshKey(k => k + 1)
        }
        prevStatus.current = s
      } catch {
        // API not reachable yet — keep polling
      }
    }

    poll()
    const id = setInterval(poll, POLL_MS)
    return () => clearInterval(id)
  }, [])

  return (
    <SyncContext.Provider value={{ status, refreshKey }}>
      {children}
    </SyncContext.Provider>
  )
}

export const useSyncStatus = () => useContext(SyncContext)
