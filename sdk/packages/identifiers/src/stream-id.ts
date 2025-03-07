import { Memoize } from 'mapmoize'
import { base36 } from 'multiformats/bases/base36'
import { CID } from 'multiformats/cid'
import { concat as uint8ArrayConcat } from 'uint8arrays'
import varint from 'varint'

import {
  STREAMID_CODEC,
  type StreamType,
  type StreamTypeCode,
  type StreamTypeName,
} from './constants.js'
import {
  fromBytes as parseFromBytes,
  fromString as parseFromString,
} from './parsing.js'
import { createCID, getCodeByName, getNameByCode } from './utils.js'

export class InvalidStreamIDBytesError extends Error {
  constructor(bytes: Uint8Array) {
    super(`Invalid StreamID bytes ${base36.encode(bytes)}: contains commit`)
  }
}

export class InvalidStreamIDStringError extends Error {
  constructor(input: string) {
    super(`Invalid StreamID string ${input}: contains commit`)
  }
}

/**
 * Parse StreamID from bytes representation.
 *
 * @param bytes - bytes representation of StreamID.
 * @throws error on invalid input
 * @see StreamID#bytes
 */
function fromBytes(bytes: Uint8Array): StreamID {
  const parsed = parseFromBytes(bytes, 'StreamID')
  if (parsed.kind === 'stream-id') {
    return new StreamID(parsed.type, parsed.init)
  }
  throw new InvalidStreamIDBytesError(bytes)
}

/**
 * Parse StreamID from string representation.
 *
 * @param input - string representation of StreamID, be it base36-encoded string or URL.
 * @see StreamID#toString
 * @see StreamID#toUrl
 */
function fromString(input: string): StreamID {
  const parsed = parseFromString(input, 'StreamID')
  if (parsed.kind === 'stream-id') {
    return new StreamID(parsed.type, parsed.init)
  }
  throw new InvalidStreamIDStringError(input)
}

const TAG = Symbol.for('@ceramic-sdk/identifiers/StreamID')

/**
 * Stream identifier, no commit information included.
 *
 * Contains stream type and CID of genesis commit.
 *
 * Encoded as `<multibase-prefix><multicodec-streamid><type><genesis-cid-bytes>`.
 *
 * String representation is base36-encoding of the bytes above.
 */
export class StreamID {
  protected readonly _tag = TAG

  private readonly _type: StreamTypeCode
  private readonly _cid: CID

  static fromBytes = fromBytes
  static fromString = fromString

  // WORKARDOUND Weird replacement for Symbol.hasInstance due to
  // this old bug in Babel https://github.com/babel/babel/issues/4452
  // which is used by CRA, which is widely popular.
  static isInstance(instance: unknown): instance is StreamID {
    return (
      instance != null &&
      typeof instance === 'object' &&
      '_tag' in instance &&
      instance._tag === TAG
    )
  }

  /**
   * Create a new StreamID.
   *
   * @param {string|number}      type       the stream type
   * @param {CID|string}         cid
   *
   * @example
   * ```typescript
   * new StreamID('MID', 'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a');
   * new StreamID('MID', cid);
   * new StreamID(3, cid);
   * ```
   */
  constructor(type: StreamType, cid: CID | string) {
    if (!(type || type === 0))
      throw new Error('StreamID constructor: type required')
    if (!cid) throw new Error('StreamID constructor: cid required')
    this._type = typeof type === 'string' ? getCodeByName(type) : type
    this._cid = typeof cid === 'string' ? CID.parse(cid) : cid
  }

  /**
   * Create a streamId from an init event payload.
   *
   * @param {string|number}         type     the stream type
   * @param {Record<string, any>}   value    the init event payload
   *
   * @example
   * ```typescript
   * const streamId = StreamID.fromPayload('MID', {
   *   header: {
   *     controllers: ['did:3:kjz...'],
   *     model: '...',
   *   }
   * });
   * ```
   */
  static fromPayload(type: StreamType, value: unknown): StreamID {
    return new StreamID(type, createCID(value))
  }

  /**
   * Stream type code
   */
  get type(): StreamTypeCode {
    return this._type
  }

  /**
   * Stream type name
   */
  @Memoize()
  get typeName(): StreamTypeName {
    return getNameByCode(this._type)
  }

  /**
   * Genesis commits CID
   */
  get cid(): CID {
    return this._cid
  }

  /**
   * Bytes representation of StreamID.
   */
  @Memoize()
  get bytes(): Uint8Array {
    const codec = new Uint8Array(varint.encode(STREAMID_CODEC))
    const type = new Uint8Array(varint.encode(this.type))
    return uint8ArrayConcat([codec, type, this.cid.bytes])
  }

  /**
   * Copy of self. Exists to maintain compatibility with CommitID.
   */
  @Memoize()
  get baseID(): StreamID {
    return new StreamID(this._type, this._cid)
  }

  /**
   * Compare equality with another StreamID.
   */
  equals(other: StreamID): boolean {
    return StreamID.isInstance(other)
      ? this.type === other.type && this.cid.equals(other.cid)
      : false
  }

  /**
   * Encode the StreamID into a string.
   */
  @Memoize()
  toString(): string {
    return base36.encode(this.bytes)
  }

  /**
   * Encode the StreamID into a base36 url.
   */
  @Memoize()
  toUrl(): string {
    return `ceramic://${this.toString()}`
  }

  /**
   * StreamId(k3y52l7mkcvtg023bt9txegccxe1bah8os3naw5asin3baf3l3t54atn0cuy98yws)
   * @internal
   */
  [Symbol.for('nodejs.util.inspect.custom')](): string {
    return `StreamID(${this.toString()})`
  }

  /**
   * String representation of StreamID.
   * @internal
   */
  [Symbol.toPrimitive](): string {
    return this.toString()
  }
}
