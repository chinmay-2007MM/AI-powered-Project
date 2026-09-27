import { create } from 'zustand'
type UIState = { selectedIndustry: string; setIndustry: (industry: string) => void }
export const useUIStore = create<UIState>((set) => ({ selectedIndustry: 'manufacturing', setIndustry: (selectedIndustry) => set({ selectedIndustry }) }))
