export const API = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'
export type Camera = { id: string; site_id: string; name: string; resolution: string | null; fps: number | null; status: string; playback_url: string | null; floorplan_x: number | null; floorplan_y: number | null }
export type Incident = { id: string; event_id: string; status: string; assigned_to: string | null; opened_at: string; resolved_at: string | null; event_type: string; severity: string; confidence: number; timestamp: string; camera_id: string; object_ids: string[]; reason_codes: string[]; evidence_ref: string | null }
export type Metrics = { cameras_total: number; cameras_online: number; active_incidents: number; events_today: number }
export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const token = localStorage.getItem('iw-token')
  const headers: Record<string, string> = { ...(token ? { Authorization: 'Bearer ' + token } : {}) }
  const hasBody = init?.body !== undefined && !(init.body instanceof FormData)
  if (hasBody) headers['Content-Type'] = 'application/json'
  Object.assign(headers, init?.headers)
  const response = await fetch(API + path, { ...init, headers })
  if (!response.ok) throw new Error(String(response.status) + ' ' + response.statusText)
  return response.json() as Promise<T>
}
export async function download(path: string, filename: string) {
  const token = localStorage.getItem('iw-token')
  const response = await fetch(API + path, { headers: token ? { Authorization: 'Bearer ' + token } : {} })
  if (!response.ok) throw new Error(String(response.status) + ' ' + response.statusText)
  const blob = await response.blob(); const url = URL.createObjectURL(blob)
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click()
  URL.revokeObjectURL(url)
}
export function socketUrl(channel: string) { const token=localStorage.getItem('iw-token')??'';return API.replace(/^http/, 'ws') + '/ws/' + channel + '?token=' + encodeURIComponent(token) }
