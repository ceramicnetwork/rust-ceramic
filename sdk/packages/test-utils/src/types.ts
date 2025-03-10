import type { Cacao } from '@didtools/cacao'

export type AuthOptions = {
  domain: string
  expirationTime?: string | null
  expiresInSecs?: number
  nonce?: string
  requestId?: string
  resources: Array<string>
  statement?: string
}

export type AuthParams = {
  domain: string
  expirationTime?: string | null
  nonce?: string
  requestId?: string
  resources: Array<string>
  statement?: string
  uri: string
}

export type AuthMethod = (options: Partial<AuthOptions>) => Promise<Cacao>
