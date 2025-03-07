import { StreamID } from '@ceramic-sdk/identifiers'
import { bases } from 'multiformats/basics'
import type { CID } from 'multiformats/cid'
import { toString as bytesToString, fromString } from 'uint8arrays'

/**
 * Maximum allowed size for IPLD blocks.
 *
 * @remarks
 * This constant is used to enforce size restrictions on blocks,
 * ensuring they do not exceed the maximum of 256 KB.
 *
 * @internal
 */
export const MAX_BLOCK_SIZE = 256000 // 256 KB

/**
 * Decodes a Base64URL-encoded string into a JSON object.
 *
 * @param value - The Base64URL-encoded string to decode.
 * @returns The decoded JSON object.
 *
 * @typeParam T - The expected shape of the returned JSON object (defaults to `Record<string, unknown>`).
 *
 * @example
 * ```typescript
 * const json = base64urlToJSON<{ foo: string }>('eyJmb28iOiAiYmFyIn0');
 * console.log(json.foo); // Output: "bar"
 * ```
 */
export function base64urlToJSON<T = Record<string, unknown>>(value: string): T {
  return JSON.parse(bytesToString(fromString(value, 'base64url')))
}

/**
 * Decodes a multibase-encoded string into a `Uint8Array`.
 *
 * @param multibaseString - The multibase-encoded string to decode.
 * @returns A `Uint8Array` containing the decoded data.
 *
 * @throws Will throw an error if the string's prefix does not match a supported multibase encoding.
 *
 * @example
 * ```typescript
 * const bytes = decodeMultibase('z1a2b3c...');
 * console.log(bytes); // Output: Uint8Array([...])
 * ```
 */
export function decodeMultibase(multibaseString: string): Uint8Array {
  const prefix = multibaseString[0]
  const baseEntry = Object.values(bases).find((base) => base.prefix === prefix)
  if (!baseEntry) {
    throw new Error(`Unsupported multibase prefix: ${prefix}`)
  }

  return baseEntry.decode(multibaseString) // Decode without the prefix.
}

/**
 * Decodes a multibase-encoded string into a JSON object.
 *
 * @param value - The multibase-encoded string to decode.
 * @returns The decoded JSON object.
 *
 * @typeParam T - The expected shape of the returned JSON object (defaults to `Record<string, unknown>`).
 *
 * @example
 * ```typescript
 * const json = decodeMultibaseToJSON<{ key: string }>('z1a2b3c...');
 * console.log(json.key); // Output: "value"
 * ```
 */
export function decodeMultibaseToJSON<T = Record<string, unknown>>(
  value: string,
): T {
  const data = decodeMultibase(value)
  return JSON.parse(new TextDecoder().decode(data))
}

/**
 * Decodes a multibase-encoded string into a `StreamID`.
 *
 * @param value - The multibase-encoded string to decode.
 * @returns A `StreamID` object representing the decoded value.
 *
 * @example
 * ```typescript
 * const streamID = decodeMultibaseToStreamID('z1a2b3c...');
 * console.log(streamID.toString()); // Output: StreamID
 * ```
 */
export function decodeMultibaseToStreamID(value: string): StreamID {
  return StreamID.fromBytes(decodeMultibase(value))
}

/**
 * Enforces a size restriction on an IPLD block.
 *
 * @param block - The IPLD block as a `Uint8Array`.
 * @param cid - The CID associated with the block.
 *
 * @throws Will throw an error if the block size exceeds the `MAX_BLOCK_SIZE`.
 *
 * @remarks
 * This function is used to ensure that blocks do not exceed the maximum allowed size.
 *
 * @example
 * ```typescript
 * const block = new Uint8Array(256001); // Larger than MAX_BLOCK_SIZE
 * try {
 *   restrictBlockSize(block, cid);
 * } catch (error) {
 *   console.error(error.message); // "Commit size exceeds the maximum block size"
 * }
 * ```
 *
 * @internal
 */
export function restrictBlockSize(block: Uint8Array, cid: CID): void {
  const size = block.byteLength
  if (size > MAX_BLOCK_SIZE) {
    throw new Error(
      `${cid} commit size ${size} exceeds the maximum block size of ${MAX_BLOCK_SIZE}`,
    )
  }
}
