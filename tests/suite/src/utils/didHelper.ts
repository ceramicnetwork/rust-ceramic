import { DID } from 'dids'
import { Ed25519Provider } from 'key-did-provider-ed25519'
import KeyDidResolver from 'key-did-resolver'
import { randomBytes } from '@stablelib/random'
import * as uint8arrays from 'uint8arrays'
import { getAuthenticatedDID } from '@didtools/key-did'

export async function createDid(seed?: string): Promise<DID> {
  const digest = seed ? uint8arrays.fromString(seed, 'base16') : randomBytes(32)
  const provider = new Ed25519Provider(digest)
  const resolver = KeyDidResolver.getResolver()
  const did = new DID({ provider, resolver })
  await did.authenticate()
  return did
}

export async function randomDID(): Promise<DID> {
  return await getAuthenticatedDID(randomBytes(32))
}
