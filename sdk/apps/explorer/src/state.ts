import { CeramicClient } from '@ceramic-sdk/http-client'
import { atom, createStore } from 'jotai'

export const store = createStore()

export const ceramicURLAtom = atom('http://localhost:5101')

export const ceramicClientAtom = atom((get) => {
  const url = get(ceramicURLAtom)
  return new CeramicClient({ url })
})

export function getCeramicClient(): CeramicClient {
  return store.get(ceramicClientAtom)
}
