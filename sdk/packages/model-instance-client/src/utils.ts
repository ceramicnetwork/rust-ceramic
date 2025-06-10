import type { StreamID } from '@ceramic-sdk/identifiers'
import {
  DocumentInitEventHeader,
  type JSONPatchOperation,
} from '@ceramic-sdk/model-instance-protocol'
import { asDIDString } from '@didtools/codecs'
import type { DID } from 'dids'
import jsonpatch from 'fast-json-patch'
import type { CID } from 'multiformats/cid'

import type { UnknownContent } from './types.js'

/**
 * Generates a random byte array of a given length.
 *
 * @param length - The length of the byte array to generate.
 * @returns A `Uint8Array` containing random bytes.
 *
 * @remarks
 * This utility is typically used for generating unique identifiers
 * or values where randomness is required.
 *
 * @internal
 */
export function randomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length)
  globalThis.crypto.getRandomValues(bytes)
  return bytes
}

/**
 * Parameters for creating a `DocumentInitEventHeader`.
 *
 * @internal
 */
export type CreateInitHeaderParams = {
  /** The stream ID of the model associated with the document. */
  model: StreamID
  /** CID of specific model version to use when validating this instance.
   * When empty the the init commit of the model is used */
  modelVersion?: CID
  /** The controller of the document. */
  controller: DID
  /** A unique value to ensure determinism, or a boolean to indicate uniqueness type. */
  unique?: Uint8Array | boolean
  /** Optional context for the document. */
  context?: StreamID
  /** Flag indicating if indexers should index the document. */
  shouldIndex?: boolean
}

/**
 * Creates a `DocumentInitEventHeader` for a ModelInstanceDocument stream.
 *
 * @param params - The parameters required to create the initialization header.
 * @returns A valid `DocumentInitEventHeader` object.
 *
 * @throws Will throw an error if the resulting header is invalid.
 *
 * @remarks
 * - The `unique` parameter determines the uniqueness type:
 *   - If `false`, a random unique value is generated.
 *   - If an `Uint8Array`, the provided value is used.
 *   - If `true` or undefined, no unique value is added.
 * - Additional optional fields like `context` and `shouldIndex` can be included.
 *
 * @internal
 */
export function createInitHeader(
  params: CreateInitHeaderParams,
): DocumentInitEventHeader {
  const did = params.controller.hasParent ? params.controller.parent : params.controller.id
  const header: DocumentInitEventHeader = {
    controllers: [asDIDString(did)],
    model: params.model,
    sep: 'model',
  }

  // Handle unique field
  if (params.unique == null || params.unique === false) {
    // Generate a random unique value (e.g., for account relation of type "list").
    header.unique = randomBytes(12)
  } else if (params.unique instanceof Uint8Array) {
    // Use the provided unique value (e.g., for account relation of type "set").
    header.unique = params.unique
  }
  // Otherwise, do not set any unique value (e.g., for account relation of type "single").

  // Add optional fields
  if (params.context != null) {
    header.context = params.context
  }
  if (params.shouldIndex != null) {
    header.shouldIndex = params.shouldIndex
  }
  if (params.modelVersion != null) {
    header.modelVersion = params.modelVersion
  }

  // Validate header before returning
  if (!DocumentInitEventHeader.is(header)) {
    throw new Error('Invalid header')
  }
  return header
}

/**
 * Computes JSON patch operations to transform one content object into another.
 *
 * @param fromContent - The current content of the document.
 * @param toContent - The new content of the document.
 * @returns An array of JSON patch operations required to transform `fromContent` into `toContent`.
 *
 * @remarks
 * - If either `fromContent` or `toContent` is undefined, an empty object `{}` is used as the default.
 * - JSON patch operations are generated using the `fast-json-patch` library.
 *
 * @example
 * ```typescript
 * const currentContent = { name: 'Alice' };
 * const newContent = { name: 'Bob' };
 * const operations = getPatchOperations(currentContent, newContent);
 * console.log(operations); // [{ op: 'replace', path: '/name', value: 'Bob' }]
 * ```
 *
 * @internal
 */
export function getPatchOperations<T extends UnknownContent = UnknownContent>(
  fromContent?: T,
  toContent?: T,
): Array<JSONPatchOperation> {
  return jsonpatch.compare(
    fromContent ?? {},
    toContent ?? {},
  ) as Array<JSONPatchOperation>
}
