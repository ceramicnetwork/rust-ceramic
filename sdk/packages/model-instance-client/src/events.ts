import {
  InitEventHeader,
  type SignedEvent,
  createSignedInitEvent,
  signEvent,
} from '@ceramic-sdk/events'
import type { CommitID, StreamID } from '@ceramic-sdk/identifiers'
import {
  type DeterministicInitEventPayload,
  type DocumentDataEventHeader,
  DocumentDataEventPayload,
  type EncodedDeterministicInitEventPayload,
  type JSONPatchOperation,
} from '@ceramic-sdk/model-instance-protocol'
import type { DID } from 'dids'
import type { CID } from 'multiformats/cid'

import type { StreamState } from '@ceramic-sdk/stream-client'
import type { UnknownContent } from './types.js'
import { createInitHeader, getPatchOperations } from './utils.js'

/**
 * Parameters required to create a non-deterministic initialization event for a ModelInstanceDocument stream.
 */
export type CreateInitEventParams<T extends UnknownContent = UnknownContent> = {
  /** Initial JSON object content for the ModelInstanceDocument stream */
  content: T | null
  /** DID controlling the ModelInstanceDocument stream */
  controller: DID
  /** Stream ID of the Model used by the ModelInstanceDocument stream */
  model: StreamID
  /** CID of specific model version to use when validating this instance.
   * When empty the the init commit of the model is used */
  modelVersion?: CID
  /** Optional context */
  context?: StreamID
  /** Flag indicating if indexers should index the ModelInstanceDocument stream (defaults to `true`) */
  shouldIndex?: boolean
}

/**
 * Creates a non-deterministic initialization event for a ModelInstanceDocument stream.
 *
 * @param params - The parameters required to create the initialization event.
 * @returns A promise that resolves to a signed initialization event.
 *
 * @remarks
 * This method creates a non-deterministic initialization event for use with streams where
 * the stream ID is not derived from a unique value.
 *
 * @see {@link getDeterministicInitEventPayload} for deterministic initialization events.
 */
export async function createInitEvent<
  T extends UnknownContent = UnknownContent,
>(params: CreateInitEventParams<T>): Promise<SignedEvent> {
  const { content, controller, ...headerParams } = params
  const header = createInitHeader({
    ...headerParams,
    controller,
    unique: false, // non-deterministic event
  })
  return await createSignedInitEvent(controller, content, header)
}

/**
 * Retrieves the payload for a deterministic initialization event for a ModelInstanceDocument stream.
 *
 * @param model - The stream ID of the model associated with the stream.
 * @param controller - The DID string or literal string for the stream's controller.
 * @param uniqueValue - Optional unique value to ensure determinism.
 * @returns The deterministic initialization event payload.
 *
 * @remarks
 * Deterministic initialization events ensure the resulting stream ID is derived
 * from the provided unique value.
 */
export function getDeterministicInitEventPayload(
  model: StreamID,
  controller: DID,
  uniqueValue?: Uint8Array,
): DeterministicInitEventPayload {
  return {
    data: null,
    header: createInitHeader({
      model,
      controller,
      unique: uniqueValue ?? true,
    }),
  }
}

/**
 * Encodes a deterministic initialization event for a ModelInstanceDocument stream.
 *
 * @param model - The stream ID of the model associated with the stream.
 * @param controller - The DID string or literal string for the stream's controller.
 * @param uniqueValue - Optional unique value to ensure determinism.
 * @returns The encoded deterministic initialization event payload.
 */
export function getDeterministicInitEvent(
  model: StreamID,
  controller: DID,
  uniqueValue?: Uint8Array,
): EncodedDeterministicInitEventPayload {
  const { header } = getDeterministicInitEventPayload(
    model,
    controller,
    uniqueValue,
  )
  // @ts-ignore: "sep" value is typed as string rather than "model" literal
  return { data: null, header: InitEventHeader.encode(header) }
}

/**
 * Creates a data event payload for a ModelInstanceDocument stream.
 *
 * @param current - The current commit ID of the stream.
 * @param data - The JSON patch operations to apply to the stream content.
 * @param header - Optional header information for the data event.
 * @returns A valid data event payload.
 *
 * @throws Will throw an error if the JSON patch operations are invalid.
 */
export function createDataEventPayload(
  current: CommitID,
  data: Array<JSONPatchOperation>,
  header?: DocumentDataEventHeader,
): DocumentDataEventPayload {
  const payload: DocumentDataEventPayload = {
    data,
    id: current.baseID.cid,
    prev: current.commit,
  }
  if (header != null) {
    payload.header = header
  }
  if (!DocumentDataEventPayload.is(payload)) {
    throw new Error('Invalid payload')
  }
  return payload
}

/**
 * Parameters required to create a signed data event for a ModelInstanceDocument stream.
 */
export type CreateDataEventParams<T extends UnknownContent = UnknownContent> = {
  /** DID controlling the ModelInstanceDocument stream */
  controller: DID
  /** Commit ID of the current tip of the ModelInstanceDocument stream */
  currentID: CommitID
  /** Current JSON object content for the stream, used with `newContent` to create a JSON patch */
  currentContent?: T
  /** New JSON object content for the stream, used with `currentContent` to create a JSON patch */
  newContent?: T
  /** Flag indicating if indexers should index the stream */
  shouldIndex?: boolean
  /** CID of specific model version to use when validating this instance.
   * When empty the the init commit of the model is used */
  modelVersion?: CID
}

/**
 * Parameters required to post a data event for a ModelInstanceDocument stream.
 */
export type PostDataEventParams<T extends UnknownContent = UnknownContent> = {
  /** String representation of the StreamID to update */
  streamID: string
  /** New JSON object content for the stream, used with `currentContent` to create a JSON patch */
  newContent: T
  /** Current JSON object containing the stream's current state */
  currentState?: StreamState
  /** Flag indicating if indexers should index the stream */
  shouldIndex?: boolean
  /** CID of specific model version to use when validating this instance.
   * When empty the the init commit of the model is used */
  modelVersion?: CID
}

/**
 * Creates a signed data event for a ModelInstanceDocument stream.
 *
 * @param params - Parameters required to create the data event.
 * @returns A promise that resolves to the signed data event.
 *
 * @remarks
 * The data event updates the content of the stream by applying JSON patch operations
 * to the existing content. The resulting event is signed by the controlling DID.
 */
export async function createDataEvent<
  T extends UnknownContent = UnknownContent,
>(params: CreateDataEventParams<T>): Promise<SignedEvent> {
  const operations = getPatchOperations(
    params.currentContent,
    params.newContent,
  )
  // Header must only be provided if there are values
  // CBOR encoding doesn't support undefined values
  let header: DocumentDataEventHeader | undefined = {}
  if (params.shouldIndex != null) {
    header.shouldIndex = params.shouldIndex
  }
  if (params.modelVersion != null) {
    header.modelVersion = params.modelVersion
  }
  if (Object.keys(header).length === 0) {
    header = undefined
  }
  const payload = createDataEventPayload(params.currentID, operations, header)
  return await signEvent(params.controller, payload)
}
