import axios from 'axios'

// In dev: Vite proxies /api → localhost:8000
// In prod: VITE_API_URL must point to the deployed backend (e.g. https://nem-dashboard-api.onrender.com)
const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL ?? '/api',
})

export const prices = {
  getSummary: (region = 'VIC1') =>
    api.get('/prices/summary', { params: { region } }).then(r => r.data),

  getMeta: (region = 'VIC1') =>
    api.get('/prices/meta', { params: { region } }).then(r => r.data),

  getData: (params = {}) =>
    api.get('/prices/', { params }).then(r => r.data),

  backfill: (year, month) =>
    api.post('/prices/backfill', { year, month }).then(r => r.data),

  sync: (startDate = null, endDate = null) =>
    api.post('/prices/sync', { start_date: startDate, end_date: endDate }, { timeout: 300_000 }).then(r => r.data),
}

export const generation = {
  getMeta: (region = 'VIC1') =>
    api.get('/generation/meta', { params: { region } }).then(r => r.data),

  getData: (params = {}) =>
    api.get('/generation/', { params }).then(r => r.data),

  getTopUnits: (params = {}) =>
    api.get('/generation/top-units', { params }).then(r => r.data),

  backfill: (year, month) =>
    api.post('/generation/backfill', { year, month }).then(r => r.data),

  sync: (startDate = null, endDate = null) =>
    api.post('/generation/sync', { start_date: startDate, end_date: endDate }, { timeout: 300_000 }).then(r => r.data),
}

export const analytics = {
  getRenewablePenetration: (params = {}) =>
    api.get('/analytics/renewable-penetration', { params }).then(r => r.data),

  getPriceDuration: (params = {}) =>
    api.get('/analytics/price-duration', { params }).then(r => r.data),
}
