import {
  InitEventPayload,
  SignedEvent,
  decodeMultibaseToJSON,
  decodeMultibaseToStreamID,
} from '@ceramic-sdk/events'
import { CommitID, StreamID } from '@ceramic-sdk/identifiers'
import {
  DocumentEvent,
  getStreamID,
} from '@ceramic-sdk/model-instance-protocol'
import { StreamClient, type StreamState } from '@ceramic-sdk/stream-client'
import type { DIDString } from '@didtools/codecs'
import type { DID } from 'dids'
import {
  type CreateDataEventParams,
  type CreateInitEventParams,
  type PostDataEventParams,
  createDataEvent,
  createInitEvent,
  getDeterministicInitEventPayload,
} from './events.js'
import type { DocumentState, UnknownContent } from './types.js'

/**
 * Parameters for creating a singleton instance of a model.
 */
export type CreateSingletonParams = {
  /** The model's stream ID */
  model: StreamID
  /** The controller of the stream (DID string or literal string) */
  controller: DIDString | string
  /** A unique value to ensure determinism of the event */
  uniqueValue?: Uint8Array
}

/**
 * Parameters for creating an instance of a model.
 */
export type CreateInstanceParams<T extends UnknownContent = UnknownContent> =
  Omit<CreateInitEventParams<T>, 'controller'> & {
    controller?: DID
  }

/**
 * Parameters for posting a data event.
 */
export type PostDataParams<T extends UnknownContent = UnknownContent> = Omit<
  CreateDataEventParams<T>,
  'controller'
> & {
  controller?: DID
}

/**
 * Parameters for updating a document with new content.
 */
export type UpdateDataParams<T extends UnknownContent = UnknownContent> = Omit<
  PostDataEventParams<T>,
  'controller'
> & {
  controller?: DID
}

/**
 * Extends the StreamClient to add functionality for interacting with Ceramic model instance documents.
 *
 * The `ModelInstanceClient` class provides methods to:
 * - Retrieve events and document states
 * - Create instances and singleton of models
 * - Update existing documents with new content
 */
export class ModelInstanceClient extends StreamClient {
  /**
   * Retrieves a `DocumentEvent` based on its commit ID.
   *
   * @param commitID - The commit ID of the event, either as a `CommitID` object or string.
   * @returns A promise that resolves to the `DocumentEvent` for the specified commit ID.
   *
   * @throws Will throw an error if the commit ID is invalid or the request fails.
   */
  async getEvent(commitID: CommitID | string): Promise<DocumentEvent> {
    const id =
      typeof commitID === 'string' ? CommitID.fromString(commitID) : commitID
    return (await this.ceramic.getEventType(
      DocumentEvent,
      id.commit.toString(),
    )) as DocumentEvent
  }

  /**
   * Creates an instance of a model with account relation single.
   * By definition this instance will always be a singleton.
   */
  async createSingleton(params: CreateSingletonParams): Promise<CommitID> {
    const event = getDeterministicInitEventPayload(
      params.model,
      params.controller,
      params.uniqueValue,
    )
    const cid = await this.ceramic.postEventType(InitEventPayload, event)
    return CommitID.fromStream(getStreamID(cid))
  }

  /**
   * Creates an instance of a model. The model must have an account relation of list or set.
   */
  async createInstance<T extends UnknownContent = UnknownContent>(
    params: CreateInstanceParams<T>,
  ): Promise<CommitID> {
    const { controller, ...rest } = params
    const event = await createInitEvent({
      ...rest,
      controller: this.getDID(controller),
    })
    const cid = await this.ceramic.postEventType(SignedEvent, event)
    return CommitID.fromStream(getStreamID(cid))
  }

  /**
   * Posts a data event and returns its commit ID.
   *
   * @param params - Parameters for posting the data event.
   * @returns A promise that resolves to the `CommitID` of the posted event.
   *
   * @remarks
   * The data event updates the content of a stream and is associated with the
   * current state of the stream.
   */
  async postData<T extends UnknownContent = UnknownContent>(
    params: PostDataParams<T>,
  ): Promise<CommitID> {
    const { controller, ...rest } = params
    const event = await createDataEvent({
      ...rest,
      controller: this.getDID(controller),
    })
    const cid = await this.ceramic.postEventType(SignedEvent, event)
    return CommitID.fromStream(params.currentID.baseID, cid)
  }

  /**
   * Retrieves the `CommitID` for the provided stream ID.
   *
   * @param streamID - The stream ID string.
   * @returns The `CommitID` for the stream.
   */
  getCurrentID(streamID: string): CommitID {
    return new CommitID(3, streamID)
  }

  /**
   * Transforms a `StreamState` into a `DocumentState`.
   *
   * @param streamState - The stream state to transform.
   * @returns The `DocumentState` derived from the stream state.
   */
  streamStateToDocumentState(streamState: StreamState): DocumentState {
    const streamID = StreamID.fromString(streamState.id)
    const decodedData = decodeMultibaseToJSON(streamState.data)
    const controller = streamState.controller
    const modelID = decodeMultibaseToStreamID(streamState.dimensions.model)
    return {
      commitID: CommitID.fromStream(streamID, streamState.event_cid),
      content: decodedData.content as UnknownContent | null,
      metadata: {
        model: modelID,
        controller: controller as DIDString,
        ...(typeof decodedData.metadata === 'object'
          ? decodedData.metadata
          : {}),
      },
    }
  }

  /**
   * Retrieves the document state for a given stream ID.
   *
   * @param streamID - The stream ID, either as a `StreamID` object or string.
   * @returns A promise that resolves to the `DocumentState`.
   *
   * @throws Will throw an error if the stream ID is invalid or the request fails.
   *
   * @remarks This method fetches the stream state using the extended StreamClient's `getStreamState` method.
   */
  async getDocumentState(streamID: StreamID | string): Promise<DocumentState> {
    const id =
      typeof streamID === 'string' ? StreamID.fromString(streamID) : streamID
    const streamState = await this.getStreamState(id)
    return this.streamStateToDocumentState(streamState)
  }

  /**
   * Updates a document with new content and returns the updated document state.
   *
   * @param params - Parameters for updating the document.
   * @returns A promise that resolves to the updated `DocumentState`.
   *
   * @remarks
   * This method posts the new content as a data event, updating the document.
   * It can optionally take the current document state to avoid re-fetching it.
   */
  async updateDocument<T extends UnknownContent = UnknownContent>(
    params: UpdateDataParams<T>,
  ): Promise<DocumentState> {
    let currentState: DocumentState
    let currentId: CommitID

    if (!params.currentState) {
      const streamState = await this.getStreamState(
        StreamID.fromString(params.streamID),
      )
      currentState = this.streamStateToDocumentState(streamState)
      currentId = this.getCurrentID(streamState.event_cid)
    } else {
      currentState = this.streamStateToDocumentState(params.currentState)
      currentId = this.getCurrentID(params.currentState.event_cid)
    }

    const { content } = currentState
    const { controller, newContent, shouldIndex, modelVersion } = params

    const newCommit = await this.postData({
      controller: this.getDID(controller),
      currentContent: content ?? undefined,
      newContent,
      currentID: currentId,
      shouldIndex,
      modelVersion,
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
}
