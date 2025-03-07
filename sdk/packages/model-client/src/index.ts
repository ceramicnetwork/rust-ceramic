import {
  type PartialInitEventHeader,
  SignedEvent,
  createSignedInitEvent,
  decodeMultibaseToJSON,
  decodeMultibaseToStreamID,
  eventToContainer,
  signEvent,
} from '@ceramic-sdk/events'
import { CommitID, StreamID } from '@ceramic-sdk/identifiers'
import {
  type JSONPatchOperation,
  MODEL,
  ModelDataEventPayload,
  type ModelDefinition,
  ModelInitEventPayload,
  type ModelMetadata,
  getModelStreamID,
} from '@ceramic-sdk/model-protocol'
import { StreamClient, type StreamState } from '@ceramic-sdk/stream-client'
import type { DIDString } from '@didtools/codecs'
import type { DID } from 'dids'
import jsonpatch from 'fast-json-patch'

const header: PartialInitEventHeader = { model: MODEL, sep: 'model' }

/**
 * Creates a signed initialization event for a model using the provided DID and model definition.
 *
 * @param did - The Decentralized Identifier (DID) to sign the initialization event.
 * @param data - The model definition to be signed.
 * @returns A promise that resolves to a `SignedEvent` representing the initialization event.
 *
 * @throws Will throw an error if the model content is invalid or the DID is not authenticated.
 */
export async function createInitEvent(
  did: DID,
  data: ModelDefinition,
): Promise<SignedEvent> {
  if (!did.authenticated) {
    await did.authenticate()
  }
  const event = await createSignedInitEvent(did, data, header)
  return event
}

/**
 * Creates a signed data event for a Model stream.
 *
 * @param params - Parameters required to create the data event.
 * @returns A promise that resolves to the signed data event.
 *
 * @remarks
 * The data event updates the content of the stream by applying JSON patch operations
 * to the existing content. The resulting event is signed by the controlling DID.
 */
export async function createDataEvent(
  params: CreateDataEventParams,
): Promise<SignedEvent> {
  const operations = getPatchOperations(
    params.currentContent,
    params.newContent,
  )
  const payload = createDataEventPayload(params.currentID, operations)
  return await signEvent(params.controller, payload)
}

/**
 * Creates a data event payload for a Model stream.
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
): ModelDataEventPayload {
  const payload: ModelDataEventPayload = {
    data,
    id: current.baseID.cid,
    prev: current.commit,
  }
  if (!ModelDataEventPayload.is(payload)) {
    throw new Error('Invalid payload')
  }
  return payload
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
export function getPatchOperations(
  fromContent?: ModelDefinition,
  toContent?: ModelDefinition,
): Array<JSONPatchOperation> {
  return jsonpatch.compare(
    fromContent ?? {},
    toContent ?? {},
  ) as Array<JSONPatchOperation>
}
/**
 * Represents a client for interacting with Ceramic models.
 *
 * The `ModelClient` class extends the `StreamClient` class to provide additional
 * methods specific to working with Ceramic models, including fetching and creating
 * model definitions, retrieving initialization events, and decoding stream data.
 */
export class ModelClient extends StreamClient {
  /**
   * Retrieves the signed initialization event of a model based on its stream ID.
   *
   * @param streamID - The stream ID of the model, either as a `StreamID` object or string.
   * @returns A promise that resolves to the `SignedEvent` for the model.
   *
   * @throws Will throw an error if the stream ID is invalid or the request fails.
   */
  async getInitEvent(streamID: StreamID | string): Promise<SignedEvent> {
    const id =
      typeof streamID === 'string' ? StreamID.fromString(streamID) : streamID
    return await this.ceramic.getEventType(SignedEvent, id.cid.toString())
  }

  /**
   * Retrieves the payload of the initialization event for a model based on its stream ID.
   *
   * @param streamID - The stream ID of the model, either as a `StreamID` object or string.
   * @param verifier - (Optional) A `DID` instance for verifying the event payload.
   * @returns A promise that resolves to the `ModelInitEventPayload`.
   *
   * @throws Will throw an error if the event or payload is invalid or verification fails.
   */
  async getPayload(
    streamID: StreamID | string,
    verifier?: DID,
  ): Promise<ModelInitEventPayload> {
    const event = await this.getInitEvent(streamID)
    const container = await eventToContainer(
      this.getDID(verifier),
      ModelInitEventPayload,
      event,
    )
    return container.payload
  }

  /**
   * Creates a model definition and returns the resulting stream ID.
   *
   * @param definition - The model JSON definition to post.
   * @param signer - (Optional) A `DID` instance for signing the model definition.
   * @returns A promise that resolves to the `StreamID` of the posted model.
   *
   * @throws Will throw an error if the definition is invalid or the signing process fails.
   */
  async createDefinition(
    definition: ModelDefinition,
    signer?: DID,
  ): Promise<StreamID> {
    const did = this.getDID(signer)
    const event = await createInitEvent(did, definition)
    const cid = await this.ceramic.postEventType(SignedEvent, event)
    return getModelStreamID(cid)
  }

  /**
   * Posts a data event to a model stream and returns its commit ID.
   *
   * @param params - Parameters for posting the data event.
   * @returns A promise that resolves to the `CommitID` of the posted event.
   *
   * @remarks
   * The data event updates the content of a stream and is associated with the
   * current state of the stream.
   */
  async postDefinition(params: PostDefinitionParams): Promise<CommitID> {
    const { controller, ...rest } = params
    const event = await createDataEvent({
      ...rest,
      controller: this.getDID(controller),
    })
    const cid = await this.ceramic.postEventType(SignedEvent, event)
    return CommitID.fromStream(params.currentID.baseID, cid)
  }

  /**
   * Updates a model with a new definition and returns the updated model state.
   * Model's can only be updated in backwards compatible ways.
   *
   * @param params - Parameters for updating the document.
   * @returns A promise that resolves to the updated `ModelState`.
   *
   * @remarks
   * This method posts the new content as a data event, updating the document.
   * It can optionally take the current document state to avoid re-fetching it.
   */
  async updateDefinition(
    params: UpdateModelDefinitionParams,
  ): Promise<ModelState> {
    let currentState: ModelState
    let currentId: CommitID

    if (!params.currentState) {
      const streamState = await this.getStreamState(
        StreamID.fromString(params.streamID),
      )
      currentState = this.streamStateToModelState(streamState)
      currentId = this.getCurrentID(streamState.event_cid)
    } else {
      currentState = this.streamStateToModelState(params.currentState)
      currentId = this.getCurrentID(params.currentState.event_cid)
    }

    const { content } = currentState
    const { controller, newContent } = params

    const newCommit = await this.postDefinition({
      controller: this.getDID(controller),
      currentContent: content ?? undefined,
      newContent,
      currentID: currentId,
    })

    return {
      commitID: newCommit,
      content: newContent,
      metadata: {
        model: currentState.metadata.model,
        controller: currentState.metadata.controller,
        ...(typeof currentState.metadata === 'object'
          ? currentState.metadata
          : {}),
      },
    }
  }

  /**
   * Retrieves the `CommitID` for the provided stream ID.
   *
   * @param streamID - The stream ID string.
   * @returns The `CommitID` for the stream.
   */
  getCurrentID(streamID: string): CommitID {
    return new CommitID(2, streamID)
  }

  /**
   * Retrieves the stringified model stream ID from a model instance document stream ID.
   *
   * @param streamID - The document stream ID, either as a `StreamID` object or string.
   * @returns A promise that resolves to the stringified model stream ID.
   *
   * @throws Will throw an error if the stream ID or its state is invalid.
   */
  async getDocumentModel(streamID: StreamID | string): Promise<string> {
    const id =
      typeof streamID === 'string' ? StreamID.fromString(streamID) : streamID
    const streamState = await this.getStreamState(id)
    const stream = decodeMultibaseToStreamID(streamState.dimensions.model)
    return stream.toString()
  }

  /**
   * Retrieves a model's JSON definition based on the model's stream ID.
   *
   * @param streamID - The stream ID of the model, either as a `StreamID` object or string.
   * @returns A promise that resolves to the `ModelDefinition` for the specified model.
   *
   * @throws Will throw an error if the stream ID is invalid or the data cannot be decoded.
   */
  async getModelDefinition(
    streamID: StreamID | string,
  ): Promise<ModelDefinition> {
    const id =
      typeof streamID === 'string' ? StreamID.fromString(streamID) : streamID
    const streamState = await this.getStreamState(id)
    const decodedData = decodeMultibaseToJSON(streamState.data)
      .content as ModelDefinition
    return decodedData
  }

  /**
   * Transforms a `StreamState` into a `ModelState`.
   *
   * @param streamState - The stream state to transform.
   * @returns The `ModelState` derived from the stream state.
   */
  streamStateToModelState(streamState: StreamState): ModelState {
    const streamID = StreamID.fromString(streamState.id)
    const decodedData = decodeMultibaseToJSON(streamState.data)
    const controller = streamState.controller
    const modelID = decodeMultibaseToStreamID(streamState.dimensions.model)
    return {
      commitID: CommitID.fromStream(streamID, streamState.event_cid),
      content: decodedData.content as ModelDefinition | null,
      metadata: {
        model: modelID,
        controller: controller as DIDString,
        ...(typeof decodedData.metadata === 'object'
          ? decodedData.metadata
          : {}),
      },
    }
  }
}

export type ModelState = {
  commitID: CommitID
  content: ModelDefinition | null
  metadata: ModelMetadata
}
/**
 * Parameters required to update a model stream.
 */
export type UpdateModelDefinitionParams = {
  /** String representation of the StreamID to update */
  streamID: string
  /** New JSON object content for the stream, used with `currentContent` to create a JSON patch */
  newContent: ModelDefinition
  /** Current model definition if known containing the stream's current state */
  currentState?: StreamState
  /** Optional `DID` instance for signing the model definition. **/
  controller?: DID
}

/**
 * Parameters for posting a data event to a model stream.
 */
export type PostDefinitionParams = Omit<CreateDataEventParams, 'controller'> & {
  controller?: DID
}

/**
 * Parameters required to create a signed data event for a Model stream.
 */
export type CreateDataEventParams = {
  /** DID controlling the ModelInstanceDocument stream */
  controller: DID
  /** Commit ID of the current tip of the ModelInstanceDocument stream */
  currentID: CommitID
  /** Current JSON object content for the stream, used with `newContent` to create a JSON patch */
  currentContent?: ModelDefinition
  /** New JSON object content for the stream, used with `currentContent` to create a JSON patch */
  newContent?: ModelDefinition
}
