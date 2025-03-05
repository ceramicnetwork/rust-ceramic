import type { AuthMethod } from '@didtools/cacao'
import { SolanaNodeAuth } from '@didtools/pkh-solana'
import {
  type KeyPairSigner,
  createSignableMessage,
  createSignerFromKeyPair,
} from '@solana/signers'
import { AccountId } from 'caip'

import { getEd25519KeyPair } from './webcrypto.ts'

// Value from RPC call - https://solana.com/docs/rpc/http/getgenesishash#code-sample
// Sliced to only keep the first 32 characters
const DEVNET_CHAIN_ID = 'EtWTRABZaYq6iMfeYKouRu166VU2xqa1'

export async function getSigner(): Promise<KeyPairSigner> {
  const keyPair = await getEd25519KeyPair()
  return await createSignerFromKeyPair(keyPair)
}

export function getAccountId(signer: KeyPairSigner): AccountId {
  return new AccountId({
    address: signer.address,
    chainId: `solana:${DEVNET_CHAIN_ID}`,
  })
}

// From @didtools/pkh-solana
export type SolanaProvider = {
  signMessage: (
    message: Uint8Array,
    type: string,
  ) => Promise<{ signature: Uint8Array }>
}

export function getProvider(signer: KeyPairSigner): SolanaProvider {
  async function signMessage(message: Uint8Array) {
    const signable = createSignableMessage(message)
    const [signatureDict] = await signer.signMessages([signable])
    const signature = signatureDict[signer.address]
    return { signature }
  }
  return { signMessage }
}

export async function getAuthMethod(): Promise<AuthMethod> {
  const signer = await getSigner()
  return await SolanaNodeAuth.getAuthMethod(
    getProvider(signer),
    getAccountId(signer),
    'test',
  )
}
