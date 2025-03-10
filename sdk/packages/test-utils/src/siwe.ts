import { Cacao, SiweMessage, type SiwxMessage } from '@didtools/cacao'
import { bytesToHex } from '@noble/hashes/utils'
import type { AccountId } from 'caip'
import type { Address, EIP1193Provider, Hex } from 'viem'

import type { AuthMethod, AuthParams } from './types.js'

/**
 * SIWX Version
 */
const VERSION = '1'

const ONE_WEEK = 7 * 24 * 60 * 60 * 1000

function randomNonce(): string {
  return bytesToHex(globalThis.crypto.getRandomValues(new Uint8Array(10)))
}

export async function createCACAO(
  provider: EIP1193Provider,
  account: AccountId,
  params: AuthParams,
): Promise<Cacao> {
  const now = new Date()

  const message: Partial<SiwxMessage> = {
    domain: params.domain,
    address: account.address,
    statement:
      params.statement ??
      'Give this application access to some of your data on Ceramic',
    uri: params.uri,
    version: VERSION,
    nonce: params.nonce ?? randomNonce(),
    issuedAt: now.toISOString(),
    chainId: account.chainId.reference,
    resources: params.resources,
  }
  // Only add expirationTime if not explicitly set to null
  if (params.expirationTime !== null) {
    message.expirationTime =
      params.expirationTime ?? new Date(now.getTime() + ONE_WEEK).toISOString()
  }

  const siweMessage = new SiweMessage(message)
  const signature = await provider.request({
    method: 'personal_sign',
    params: [
      siweMessage.signMessage({ eip55: true }) as Hex,
      account.address as Address,
    ],
  })
  siweMessage.signature = signature

  return Cacao.fromSiweMessage(siweMessage)
}

export function createAuthMethod(
  provider: EIP1193Provider,
  account: AccountId,
): AuthMethod {
  return (params) => {
    return createCACAO(provider, account, params as unknown as AuthParams)
  }
}
