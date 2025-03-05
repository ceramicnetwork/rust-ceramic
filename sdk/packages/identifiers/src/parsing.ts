import { base36 } from 'multiformats/bases/base36'
import { CID } from 'multiformats/cid'
import { decode as decodeMultiHash } from 'multiformats/hashes/digest'
import varint from 'varint'

import { STREAMID_CODEC, type StreamTypeCode } from './constants.js'

/** @internal */
export function readVarint(bytes: Uint8Array): [number, Uint8Array, number] {
  const value = varint.decode(bytes)
  const readLength = varint.decode.bytes
  if (readLength == null) {
    throw new Error('No remaining bytes to decode')
  }
  const remainder = bytes.subarray(readLength)
  return [value, remainder, readLength]
}

function isCidVersion(input: number): input is 0 | 1 {
  return input === 0 || input === 1
}

/** @internal */
export function readCid(bytes: Uint8Array): [CID, Uint8Array] {
  const [cidVersion, cidVersionRemainder] = readVarint(bytes)
  if (!isCidVersion(cidVersion)) {
    throw new Error(`Unknown CID version ${cidVersion}`)
  }
  const [codec, codecRemainder] = readVarint(cidVersionRemainder)
  const [, mhCodecRemainder, mhCodecLength] = readVarint(codecRemainder)
  const [mhLength, , mhLengthLength] = readVarint(mhCodecRemainder)
  const multihashBytes = codecRemainder.subarray(
    0,
    mhCodecLength + mhLengthLength + mhLength,
  )
  const multihashBytesRemainder = codecRemainder.subarray(
    mhCodecLength + mhLengthLength + mhLength,
  )
  return [
    CID.create(cidVersion, codec, decodeMultiHash(multihashBytes)),
    multihashBytesRemainder,
  ]
}

/** @internal */
export type StreamIDComponents = {
  kind: 'stream-id'
  type: StreamTypeCode
  init: CID
}

/** @internal */
export type CommitIDComponents = {
  kind: 'commit-id'
  type: StreamTypeCode
  init: CID
  commit: CID | null
}

/** @internal */
export type StreamRefComponents = StreamIDComponents | CommitIDComponents

/**
 * Parse StreamID or CommitID from bytes.
 * @internal
 */
export function fromBytes(
  input: Uint8Array,
  title = 'StreamRef',
): StreamRefComponents {
  const [streamCodec, streamCodecRemainder] = readVarint(input)
  if (streamCodec !== STREAMID_CODEC)
    throw new Error(`Invalid ${title}, does not include streamid codec`)
  const [code, streamtypeRemainder] = readVarint(streamCodecRemainder)
  const type = code as StreamTypeCode
  const cidResult = readCid(streamtypeRemainder)
  const [init, remainder] = cidResult
  if (remainder.length === 0) {
    return { kind: 'stream-id', type, init }
  }
  if (remainder.length === 1 && remainder[0] === 0) {
    // Zero commit
    return { kind: 'commit-id', type, init, commit: null }
  }
  // Commit
  const [commit] = readCid(remainder)
  return { kind: 'commit-id', type, init, commit }
}

/**
 * RegExp to match against URL representation of StreamID or CommitID.
 */
const URL_PATTERN =
  /(ceramic:\/\/|\/ceramic\/)?([a-zA-Z0-9]+)(\?commit=([a-zA-Z0-9]+))?/

/** @internal */
export function fromString(
  input: string,
  title = 'StreamRef',
): StreamRefComponents {
  const protocolMatch = URL_PATTERN.exec(input) || []
  const base = protocolMatch[2]
  if (!base) throw new Error(`Malformed ${title} string: ${input}`)
  const bytes = base36.decode(base)
  const streamRef = fromBytes(bytes)
  const commit = protocolMatch[4]
  if (commit) {
    return {
      kind: 'commit-id',
      type: streamRef.type,
      init: streamRef.init,
      commit: parseCommit(streamRef.init, commit),
    }
  }
  return streamRef
}

/**
 * Safely parse CID, be it CID instance or a string representation.
 * Return `undefined` if not CID.
 *
 * @param input - CID or string.
 *
 * @internal
 */
export function parseCID(input: unknown): CID | null {
  try {
    return typeof input === 'string' ? CID.parse(input) : CID.asCID(input)
  } catch {
    return null
  }
}

/**
 * Parse commit CID from string or number.
 * `null` result indicates genesis commit.
 * If `commit` is 0, `'0'`, `null` or is equal to `genesis` CID, result is `null`.
 * Otherwise, return commit as proper CID.
 *
 * @param genesis - genesis CID for stream
 * @param commit - representation of commit, be it CID, 0, `'0'`, `null`
 *
 * @internal
 */
export function parseCommit(
  genesis: CID,
  commit: CID | string | number | null = null,
): CID | null {
  if (!commit) return null // Handle number 0, null and undefined
  if (commit === '0') return null // Handle string 0

  const commitCID = parseCID(commit)
  if (commitCID) {
    // CID-like
    if (genesis.equals(commitCID)) {
      return null
    }
    return commitCID
  }
  throw new Error(
    'Cannot specify commit as a number except to request commit 0 (the genesis commit)',
  )
}
