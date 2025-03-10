import type { AccountId } from 'caip'
import { DIDSession } from 'did-session'
import type { EIP1193Provider, Hex } from 'viem'
import { generatePrivateKey } from 'viem/accounts'

import { createProvider, getAccount } from './provider.js'
import { createAuthMethod } from './siwe.js'
import type { AuthMethod, AuthOptions } from './types.js'

// Re-exports from viem
export { generatePrivateKey }
export type { EIP1193Provider, Hex }

// Re-exports useful internals
export { createAuthMethod, createProvider, getAccount }

export type EthereumDIDParams = {
  accountId: AccountId
  authMethod: AuthMethod
  authOptions: AuthOptions
}

export class EthereumDID {
  static async fromProvider(
    provider: EIP1193Provider,
    authOptions: AuthOptions,
  ): Promise<EthereumDID> {
    const accountId = await getAccount(provider)
    const authMethod = createAuthMethod(provider, accountId)
    return new EthereumDID({ accountId, authMethod, authOptions })
  }

  static async fromPrivateKey(
    privateKey: Hex,
    authOptions: AuthOptions,
  ): Promise<EthereumDID> {
    return await EthereumDID.fromProvider(
      createProvider(privateKey),
      authOptions,
    )
  }

  static async random(authOptions: AuthOptions): Promise<EthereumDID> {
    return await EthereumDID.fromPrivateKey(generatePrivateKey(), authOptions)
  }

  #authMethod: AuthMethod
  #authOptions: AuthOptions
  #id: string

  constructor(params: EthereumDIDParams) {
    if (params.authOptions.resources.length === 0) {
      throw new Error(
        `The "resources" array of the authorization options must contain at least one item`,
      )
    }
    this.#authMethod = params.authMethod
    this.#authOptions = params.authOptions
    this.#id = `did:pkh:${params.accountId.toString()}`
  }

  get id(): string {
    return this.#id
  }

  async createSession(options: Partial<AuthOptions> = {}): Promise<DIDSession> {
    return await DIDSession.authorize(this.#authMethod, {
      ...this.#authOptions,
      ...options,
      // biome-ignore lint/suspicious/noExplicitAny: AuthOpts handling
    } as any)
  }
}
