import {
  type Hex,
  createAuthMethod,
  createProvider,
  getAccount,
} from '@ceramic-sdk/test-utils'
import type { AuthMethod } from '@didtools/cacao'

const PRIVATE_KEY =
  '0xe50df915de22bad5bf1abf43f78b55d64640afdcdfa6b1699a514d97662b23f7' as Hex

export async function getAuthMethod(): Promise<AuthMethod> {
  const provider = createProvider(PRIVATE_KEY)
  const accountId = await getAccount(provider)
  return createAuthMethod(provider, accountId)
}
