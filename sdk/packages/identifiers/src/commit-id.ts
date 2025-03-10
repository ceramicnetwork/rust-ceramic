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
  parseCommit,
  fromBytes as parseFromBytes,
  fromString as parseFromString,
} from './parsing.js'
import { StreamID } from './stream-id.js'
import { getCodeByName, getNameByCode } from './utils.js'

export class InvalidCommitIDBytesError extends Error {
  constructor(bytes: Uint8Array) {
    super(
      `Error while parsing CommitID from bytes ${base36.encode(
        bytes,
      )}: no commit information provided`,
    )
  }
}

export class InvalidCommitIDStringError extends Error {
  constructor(input: string) {
    super(
      `Error while parsing CommitID from string ${input}: no commit information provided`,
    )
  }
}

/**
 * Parse CommitID from bytes representation.
 *
 * @param bytes - bytes representation of CommitID.
 * @throws error on invalid input
 * @see CommitID#bytes
 */
function fromBytes(bytes: Uint8Array): CommitID {
  const parsed = parseFromBytes(bytes, 'CommitID')
  if (parsed.kind === 'commit-id') {
    return new CommitID(parsed.type, parsed.init, parsed.commit)
  }
  throw new InvalidCommitIDBytesError(bytes)
}

/**
 * Parse CommitID from string representation.
 *
 * @param input - string representation of CommitID, be it base36-encoded string or URL.
 * @see CommitID#toString
 * @see CommitID#toUrl
 */
function fromString(input: string): CommitID {
  const parsed = parseFromString(input, 'CommitID')
  if (parsed.kind === 'commit-id') {
    return new CommitID(parsed.type, parsed.init, parsed.commit)
  }
  throw new InvalidCommitIDStringError(input)
}

const TAG = Symbol.for('@ceramic-sdk/identifiers/CommitID')

/**
 * Commit identifier, includes type, init CID, commit CID.
 *
 * Encoded as `<multibase-prefix><multicodec-streamid><type><init-cid-bytes><commit-cid-bytes>`.
 *
 * String representation is base36-encoding of the bytes above.
 */
export class CommitID {
  protected readonly _tag = TAG

  readonly #type: StreamTypeCode
  readonly #cid: CID
  readonly #commit: CID | null // null = init commit

  static fromBytes = fromBytes
  static fromString = fromString

  /**
   * Construct new CommitID for a given stream and commit
   */
  static fromStream(
    stream: StreamID,
    commit: CID | string | number | null = null,
  ): CommitID {
    return new CommitID(stream.type, stream.cid, commit)
  }

  // WORKAROUND. Weird replacement for Symbol.hasInstance due to
  // this old bug in Babel https://github.com/babel/babel/issues/4452
  // which is used by CRA, which is widely popular.
  static isInstance(instance: unknown): instance is CommitID {
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
   * @param type
   * @param {CID|string}         cid
   * @param {CID|string}         commit CID. Pass '0', 0, or omit the value as shorthand for the init commit.
   *
   * @example
   * new StreamID(<type>, <CID>|<cidStr>)
   * new StreamID(<type>, <CID>|<cidStr>, <CommitCID>|<CommitCidStr>)
   */
  constructor(
    type: StreamType,
    cid: CID | string,
    commit: CID | string | number | null = null,
  ) {
    if (!type && type !== 0) throw new Error('constructor: type required')
    if (!cid) throw new Error('constructor: cid required')
    this.#type = typeof type === 'string' ? getCodeByName(type) : type
    this.#cid = typeof cid === 'string' ? CID.parse(cid) : cid
    this.#commit = parseCommit(this.#cid, commit)
  }

  /**
   * Construct StreamID, no commit information included
   */
  @Memoize()
  get baseID(): StreamID {
    return new StreamID(this.#type, this.#cid)
  }

  /**
   * Stream type code
   */
  get type(): StreamTypeCode {
    return this.#type
  }

  /**
   * Stream type name
   */
  @Memoize()
  get typeName(): StreamTypeName {
    return getNameByCode(this.#type)
  }

  /**
   * Genesis CID
   */
  get cid(): CID {
    return this.#cid
  }

  /**
   * Commit CID
   */
  @Memoize()
  get commit(): CID {
    return this.#commit || this.#cid
  }

  /**
   * Bytes representation
   */
  @Memoize()
  get bytes(): Uint8Array {
    const codec = new Uint8Array(varint.encode(STREAMID_CODEC))
    const type = new Uint8Array(varint.encode(this.type))

    const commitBytes = this.#commit?.bytes || new Uint8Array([0])
    return uint8ArrayConcat([codec, type, this.cid.bytes, commitBytes])
  }

  /**
   * Compare equality with another CommitID.
   */
  equals(other: CommitID): boolean {
    return (
      this.type === other.type &&
      this.cid.equals(other.cid) &&
      this.commit.equals(other.commit)
    )
  }

  /**
   * Encode the CommitID into a string.
   */
  @Memoize()
  toString(): string {
    return base36.encode(this.bytes)
  }

  /**
   * Encode the StreamID into a base36 url
   */
  @Memoize()
  toUrl(): string {
    return `ceramic://${this.toString()}`
  }

  /**
   * If init commit:
   * CommitID(k3y52l7mkcvtg023bt9txegccxe1bah8os3naw5asin3baf3l3t54atn0cuy98yws, 0)
   *
   * If commit:
   * CommitID(k3y52l7mkcvtg023bt9txegccxe1bah8os3naw5asin3baf3l3t54atn0cuy98yws, bagjqcgzaday6dzalvmy5ady2m5a5legq5zrbsnlxfc2bfxej532ds7htpova)
   *
   * @internal
   */
  [Symbol.for('nodejs.util.inspect.custom')](): string {
    return `CommitID(${this.toString()})`
  }

  /**
   * String representation of CommitID.
   * @internal
   */
  [Symbol.toPrimitive](): string | Uint8Array {
    return this.toString()
  }
}
