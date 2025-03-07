import * as codec from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import { type Digest, create } from 'multiformats/hashes/digest'
import { sha256 } from 'multiformats/hashes/sha2'

import {
  STREAM_TYPES,
  type StreamType,
  type StreamTypeCode,
  type StreamTypeName,
} from './constants.js'
import { StreamID } from './stream-id.js'

type CodecCode = typeof codec.code
type DigestCode = typeof sha256.code

/** @internal */
export function createCID<T = unknown>(
  value: T,
): CID<T, CodecCode, DigestCode, 1> {
  const bytes = codec.encode(value)
  // digest call is synchronous, no need to await
  const hash = sha256.digest(bytes) as Digest<DigestCode, number>
  return CID.createV1(codec.code, hash) as CID<T, CodecCode, DigestCode, 1>
}

/** @internal */
export function getCodeByName(name: StreamTypeName): StreamTypeCode {
  const index = STREAM_TYPES[name]
  if (index == null) {
    throw new Error(`No stream type registered for name ${name}`)
  }
  return index
}

/** @internal */
export function getNameByCode(code: StreamTypeCode): StreamTypeName {
  for (const [name, value] of Object.entries(STREAM_TYPES)) {
    if (value === code) {
      return name as StreamTypeName
    }
  }
  throw new Error(`No stream type registered for code ${code}`)
}

/** @internal */
export function randomCID(): CID {
  const randomBytes = globalThis.crypto.getRandomValues(new Uint8Array(32))
  return CID.createV1(codec.code, create(sha256.code, randomBytes))
}

/** @internal */
export function randomStreamID(type: StreamType = 0): StreamID {
  return new StreamID(type, randomCID())
}
