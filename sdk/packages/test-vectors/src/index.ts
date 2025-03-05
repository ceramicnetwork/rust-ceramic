import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import * as cborCodec from '@ipld/dag-cbor'
import * as jsonCodec from '@ipld/dag-json'
import { type CAR, CARFactory } from 'cartonne'
import * as joseCodec from 'dag-jose'
import type { CID } from 'multiformats/cid'
import { sha256 } from 'multihashes-sync/sha2'

const CONTROLLER_TYPES = {
  'key-ecdsa-p256': true,
  'key-ed25519': true,
  'pkh-ethereum': true,
  'pkh-solana': true,
  'pkh-webauthn': true,
}

export type ControllerType = keyof typeof CONTROLLER_TYPES

export type ArchiveRootContentCommon = {
  controller: string
  model: Uint8Array
  validInitEvent: CID
  validDataEvent: CID
  validDeterministicEvent: CID
  validInitPayload: CID
  validDataPayload: CID
  invalidInitEventSignature: CID
  invalidDataEventSignature: CID
}

export type ArchiveRootContentWithoutCapability = ArchiveRootContentCommon & {
  withCapability: false
}

export type ArchiveRootContentWithCapability = ArchiveRootContentCommon & {
  withCapability: true
  expiredInitEventCapability: CID
  expiredDataEventCapability: CID
  invalidInitEventCapabilitySignature: CID
  invalidDataEventCapabilitySignature: CID
  invalidInitEventCapabilityNoResource: CID
  invalidDataEventCapabilityNoResource: CID
  invalidInitEventCapabilityOtherModel: CID
  invalidDataEventCapabilityOtherModel: CID
  validInitEventCapabilityExactModel: CID
  validDataEventCapabilityExactModel: CID
}

export type ArchiveRootContent =
  | ArchiveRootContentWithCapability
  | ArchiveRootContentWithoutCapability

export function getCARPath(type: ControllerType): string {
  if (CONTROLLER_TYPES[type] == null) {
    throw new Error(`Unsupported CAR path: ${type}`)
  }
  const url = new URL(`../assets/${type}.car`, import.meta.url)
  return fileURLToPath(url)
}

export const carFactory = new CARFactory()
carFactory.codecs.add(cborCodec)
carFactory.codecs.add(jsonCodec)
carFactory.codecs.add(joseCodec)
carFactory.hashers.add(sha256)

export async function loadCAR(type: ControllerType): Promise<CAR> {
  const bytes = await readFile(getCARPath(type))
  return carFactory.fromBytes(bytes)
}
