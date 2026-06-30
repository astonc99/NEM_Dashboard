import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { SyncProvider } from './context/SyncContext'
import Layout from './components/Layout'
import Home from './pages/Home'
import Prices from './pages/Prices'
import GenerationMix from './pages/GenerationMix'
import Analytics from './pages/Analytics'

export default function App() {
  return (
    <SyncProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<Home />} />
            <Route path="prices"     element={<Prices />} />
            <Route path="generation" element={<GenerationMix />} />
            <Route path="analytics"  element={<Analytics />} />
          </Route>
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </SyncProvider>
  )
}
