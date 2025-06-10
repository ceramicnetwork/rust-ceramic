import { assertSignedEvent, getSignedEventPayload } from '@ceramic-sdk/events'
import type { CeramicClient } from '@ceramic-sdk/http-client'
import {
  CommitID,
  StreamID,
  randomCID,
  randomStreamID,
} from '@ceramic-sdk/identifiers'
import {
  DataInitEventPayload,
  DocumentDataEventPayload,
  DocumentEvent,
  STREAM_TYPE_ID,
} from '@ceramic-sdk/model-instance-protocol'
import { getAuthenticatedDID } from '@didtools/key-did'
import { jest } from '@jest/globals'
import { equals } from 'uint8arrays'

import {
  ModelInstanceClient,
  createDataEvent,
  createInitEvent,
  getDeterministicInitEvent,
  getDeterministicInitEventPayload,
} from '../src/index.js'

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

describe('getDeterministicInitEventPayload()', () => {
  test('returns the deterministic event payload without unique value by default', () => {
    const model = randomStreamID()
    const event = getDeterministicInitEventPayload(model, authenticatedDID)
    expect(event.data).toBeNull()
    expect(event.header.controllers).toEqual([
      'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
    ])
    expect(event.header.model).toBe(model)
    expect(event.header.unique).toBeUndefined()
  })

  test('returns the deterministic event payload with the provided unique value', () => {
    const model = randomStreamID()
    const unique = new Uint8Array([0, 1, 2])
    const event = getDeterministicInitEventPayload(
      model,
      authenticatedDID,
      unique,
    )
    expect(event.data).toBeNull()
    expect(event.header.controllers).toEqual([
      'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
    ])
    expect(event.header.model).toBe(model)
    expect(event.header.unique).toBe(unique)
  })
})

describe('getDeterministicInitEvent()', () => {
  test('returns the deterministic event without unique value by default', () => {
    const model = randomStreamID()
    const event = getDeterministicInitEvent(model, authenticatedDID)
    expect(event.data).toBeNull()
    expect(event.header.controllers).toEqual([
      'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
    ])
    expect(equals(event.header.model, model.bytes)).toBe(true)
    expect(event.header.unique).toBeUndefined()
  })

  test('returns the deterministic event with the provided unique value', () => {
    const model = randomStreamID()
    const unique = new Uint8Array([0, 1, 2])
    const event = getDeterministicInitEvent(model, authenticatedDID, unique)
    expect(event.data).toBeNull()
    expect(event.header.controllers).toEqual([
      'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
    ])
    expect(equals(event.header.model, model.bytes)).toBe(true)
    expect(event.header.unique).toBe(unique)
  })
})

describe('createInitEvent()', () => {
  test('creates unique events by adding a random unique value', async () => {
    const model = randomStreamID()
    const event1 = await createInitEvent({
      content: { hello: 'world' },
      controller: authenticatedDID,
      model,
    })
    assertSignedEvent(event1)

    const event2 = await createInitEvent({
      content: { hello: 'world' },
      controller: authenticatedDID,
      model,
    })
    expect(event2).not.toEqual(event1)
  })

  test('adds the context and shouldIndex when if provided', async () => {
    const model = randomStreamID()
    const event1 = await createInitEvent({
      content: { hello: 'world' },
      controller: authenticatedDID,
      model,
    })
    const payload1 = await getSignedEventPayload(DataInitEventPayload, event1)
    expect(payload1.header.context).toBeUndefined()
    expect(payload1.header.shouldIndex).toBeUndefined()

    const context = randomStreamID()
    const event2 = await createInitEvent({
      content: { hello: 'world' },
      controller: authenticatedDID,
      model,
      context,
      shouldIndex: true,
    })
    const payload2 = await getSignedEventPayload(DataInitEventPayload, event2)
    expect(payload2.header.context?.equals(context)).toBe(true)
    expect(payload2.header.shouldIndex).toBe(true)
  })
})

describe('createDataEvent()', () => {
  const commitID = CommitID.fromStream(randomStreamID(), randomCID())

  test('creates the JSON patch payload', async () => {
    const event = await createDataEvent({
      controller: authenticatedDID,
      currentID: commitID,
      currentContent: { hello: 'test' },
      newContent: { hello: 'world', test: true },
    })
    const payload = await getSignedEventPayload(DocumentDataEventPayload, event)
    expect(payload.data).toEqual([
      { op: 'replace', path: '/hello', value: 'world' },
      { op: 'add', path: '/test', value: true },
    ])
    expect(payload.header).toBeUndefined()
  })

  test('adds the shouldIndex header when provided', async () => {
    const event = await createDataEvent({
      controller: authenticatedDID,
      currentID: commitID,
      newContent: { hello: 'world' },
      shouldIndex: true,
    })
    const payload = await getSignedEventPayload(DocumentDataEventPayload, event)
    expect(payload.header).toEqual({ shouldIndex: true })
  })
})

describe('ModelInstanceClient', () => {
  describe('getEvent() method', () => {
    test('gets a MID event by commit ID', async () => {
      const streamID = randomStreamID()
      const docEvent = getDeterministicInitEvent(streamID, authenticatedDID)
      const getEventType = jest.fn(() => docEvent)
      const ceramic = { getEventType } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })

      const commitID = CommitID.fromStream(streamID)
      const event = await client.getEvent(CommitID.fromStream(streamID))
      expect(getEventType).toHaveBeenCalledWith(
        DocumentEvent,
        commitID.cid.toString(),
      )
      expect(event).toBe(docEvent)
    })
  })

  describe('createSingleton() method', () => {
    test('creates singleton instance and returns the CommitID', async () => {
      const postEventType = jest.fn(() => randomCID())
      const ceramic = { postEventType } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })

      const id = await client.createSingleton({
        controller: authenticatedDID,
        model: randomStreamID(),
      })
      expect(postEventType).toHaveBeenCalled()
      expect(id).toBeInstanceOf(CommitID)
      expect(id.baseID.type).toBe(STREAM_TYPE_ID)
    })
  })

  describe('createInstance() method', () => {
    test('creates instance and returns the CommitID', async () => {
      const postEventType = jest.fn(() => randomCID())
      const ceramic = { postEventType } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })

      const id = await client.createInstance({
        content: { test: true },
        controller: authenticatedDID,
        model: randomStreamID(),
      })
      expect(postEventType).toHaveBeenCalled()
      expect(id).toBeInstanceOf(CommitID)
      expect(id.baseID.type).toBe(STREAM_TYPE_ID)
    })
  })

  describe('postData() method', () => {
    test('posts the signed data event and returns the CommitID', async () => {
      const postEventType = jest.fn(() => randomCID())
      const ceramic = { postEventType } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })

      const initCommitID = await client.createInstance({
        content: { test: 0 },
        controller: authenticatedDID,
        model: randomStreamID(),
      })
      expect(postEventType).toHaveBeenCalledTimes(1)

      const dataCommitID = await client.postData({
        controller: authenticatedDID,
        currentID: initCommitID,
        newContent: { test: 1 },
      })
      expect(dataCommitID).toBeInstanceOf(CommitID)
      expect(dataCommitID.baseID.equals(initCommitID.baseID)).toBe(true)
    })
  })

  describe('getDocumentState() method', () => {
    test('gets the document state by stream ID', async () => {
      const mockStreamState = {
        id: 'k2t6wyfsu4pfy7r1jdd6jex9oxbqyp4gr2a5kxs8ioxwtisg8nzj3anbckji8g',
        event_cid:
          'bafyreib5j4def5a4w4j6sg4upm6nb4cfn752wdjwqtwdzejfladyyymxca',
        controller: 'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
        dimensions: {
          context: 'u',
          controller:
            'uZGlkOmtleTp6Nk1raVRCejF5bXVlcEFRNEhFSFlTRjFIOHF1RzVHTFZWUVIzZGpkWDNtRG9vV3A',
          model: 'uzgEAAXESIA8og02Dnbwed_besT8M0YOnaZ-hrmMZaa7mnpdUL8jE',
        },
        data: 'ueyJtZXRhZGF0YSI6eyJzaG91bGRJbmRleCI6dHJ1ZX0sImNvbnRlbnQiOnsiYm9keSI6IlRoaXMgaXMgYSBzaW1wbGUgbWVzc2FnZSJ9fQ',
      }
      const docState = {
        content: { body: 'This is a simple message' },
        metadata: {
          shouldIndex: true,
          controller:
            'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
          model:
            'k2t6wyfsu4pfx2cbha7xh9fsjvqr8b7g3w7365w627bup0l5qo020e2id4txvo',
        },
      }
      const streamId =
        'k2t6wyfsu4pfy7r1jdd6jex9oxbqyp4gr2a5kxs8ioxwtisg8nzj3anbckji8g'
      // Mock CeramicClient and its API
      const mockGet = jest.fn(() =>
        Promise.resolve({
          data: mockStreamState,
          error: null,
        }),
      )
      const ceramic = {
        api: { GET: mockGet },
      } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })

      const documentState = await client.getDocumentState(
        StreamID.fromString(streamId),
      )
      expect(documentState.content).toEqual(docState.content)
      expect(documentState.metadata.model.toString()).toEqual(
        docState.metadata.model,
      )
    })
  })
  describe('updateDocument() method', () => {
    const mockStreamState = {
      id: 'k2t6wyfsu4pfy7r1jdd6jex9oxbqyp4gr2a5kxs8ioxwtisg8nzj3anbckji8g',
      event_cid: 'bafyreib5j4def5a4w4j6sg4upm6nb4cfn752wdjwqtwdzejfladyyymxca',
      controller: 'did:key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp',
      dimensions: {
        context: 'u',
        controller:
          'uZGlkOmtleTp6Nk1raVRCejF5bXVlcEFRNEhFSFlTRjFIOHF1RzVHTFZWUVIzZGpkWDNtRG9vV3A',
        model: 'uzgEAAXESIA8og02Dnbwed_besT8M0YOnaZ-hrmMZaa7mnpdUL8jE',
      },
      data: 'ueyJtZXRhZGF0YSI6eyJzaG91bGRJbmRleCI6dHJ1ZX0sImNvbnRlbnQiOnsiYm9keSI6IlRoaXMgaXMgYSBzaW1wbGUgbWVzc2FnZSJ9fQ',
    }
    test('updates a document with new content when current is not provided', async () => {
      const newContent = { body: 'This is a new message' }
      const streamId =
        'k2t6wyfsu4pfy7r1jdd6jex9oxbqyp4gr2a5kxs8ioxwtisg8nzj3anbckji8g'
      // Mock CeramicClient and its API
      const postEventType = jest.fn(() => randomCID())
      const mockGet = jest.fn(() =>
        Promise.resolve({
          data: mockStreamState,
          error: null,
        }),
      )
      const ceramic = {
        api: { GET: mockGet },
        postEventType,
      } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })
      jest.spyOn(client, 'getDocumentState')
      jest.spyOn(client, 'postData')
      const newState = await client.updateDocument({
        streamID: streamId,
        newContent,
        shouldIndex: true,
      })
      expect(client.postData).toHaveBeenCalledWith({
        controller: authenticatedDID,
        currentID: new CommitID(3, mockStreamState.event_cid),
        currentContent: { body: 'This is a simple message' },
        newContent,
        shouldIndex: true,
      })
      expect(newState.content).toEqual(newContent)
      expect(postEventType).toHaveBeenCalled()
      expect(mockGet).toHaveBeenCalledWith('/streams/{stream_id}', {
        params: { path: { stream_id: streamId } },
      })
    })
    test('updates a document with new content when current is provided', async () => {
      const newContent = { body: 'This is a new message' }
      const streamId =
        'k2t6wyfsu4pfy7r1jdd6jex9oxbqyp4gr2a5kxs8ioxwtisg8nzj3anbckji8g'
      // Mock CeramicClient and its API
      const postEventType = jest.fn(() => randomCID())
      const mockGet = jest.fn(() =>
        Promise.resolve({
          data: mockStreamState,
          error: null,
        }),
      )

      const ceramic = {
        api: { GET: mockGet },
        postEventType,
      } as unknown as CeramicClient
      const client = new ModelInstanceClient({ ceramic, did: authenticatedDID })
      jest.spyOn(client, 'streamStateToDocumentState')
      const newState = await client.updateDocument({
        streamID: streamId,
        newContent,
        currentState: mockStreamState,
      })
      expect(client.streamStateToDocumentState).toHaveBeenCalled()
      expect(newState.content).toEqual(newContent)
      expect(postEventType).toHaveBeenCalled()
      expect(mockGet).not.toHaveBeenCalled()
    })
  })
})
