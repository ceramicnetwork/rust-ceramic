import { SignedEvent, assertSignedEvent } from '@ceramic-sdk/events'
import type { CeramicClient } from '@ceramic-sdk/http-client'
import { StreamID, randomCID, randomStreamID } from '@ceramic-sdk/identifiers'
import {
  MODEL_RESOURCE_URI,
  type ModelDefinition,
  STREAM_TYPE_ID,
} from '@ceramic-sdk/model-protocol'
import { EthereumDID } from '@ceramic-sdk/test-utils'
import { getAuthenticatedDID } from '@didtools/key-did'
import { jest } from '@jest/globals'

import { ModelClient, createInitEvent } from '../src/index.js'

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

const testModelV1: ModelDefinition = {
  version: '1.0',
  name: 'TestModel',
  description: 'Test model',
  accountRelation: { type: 'list' },
  schema: {
    type: 'object',
    properties: {
      test: { type: 'string', maxLength: 10 },
    },
    additionalProperties: false,
  },
}

describe('createInitEvent()', () => {
  describe('support controllers', () => {
    test('supports did:key', async () => {
      expect(authenticatedDID.id.startsWith('did:key')).toBe(true)
      await expect(
        createInitEvent(authenticatedDID, testModelV1),
      ).resolves.not.toThrow()
    })

    test('supports did:pkh', async () => {
      const did = await EthereumDID.random({
        domain: 'test',
        resources: [MODEL_RESOURCE_URI],
      })

      const validSession = await did.createSession({ expirationTime: null })

      await expect(
        createInitEvent(validSession.did, testModelV1),
      ).resolves.not.toThrow()
    })
  })

  test('returns the signed event', async () => {
    const event = await createInitEvent(authenticatedDID, testModelV1)
    assertSignedEvent(event)
  })
})

describe('ModelClient', () => {
  describe('getInitEvent() method', () => {
    test('gets the model init event', async () => {
      const modelEvent = await createInitEvent(authenticatedDID, testModelV1)
      const getEventType = jest.fn(() => modelEvent)
      const ceramic = { getEventType } as unknown as CeramicClient
      const client = new ModelClient({ ceramic, did: authenticatedDID })

      const modelID = randomStreamID()
      const event = await client.getInitEvent(modelID)
      expect(getEventType).toHaveBeenCalledWith(
        SignedEvent,
        modelID.cid.toString(),
      )
      expect(event).toBe(modelEvent)
    })
  })

  describe('getPayload() method', () => {
    test('gets the model init event payload', async () => {
      const modelEvent = await createInitEvent(authenticatedDID, testModelV1)
      const getEventType = jest.fn(() => modelEvent)
      const ceramic = { getEventType } as unknown as CeramicClient
      const client = new ModelClient({ ceramic, did: authenticatedDID })

      const modelID = randomStreamID()
      const payload = await client.getPayload(modelID)
      expect(getEventType).toHaveBeenCalledWith(
        SignedEvent,
        modelID.cid.toString(),
      )
      expect(payload.header.controllers).toEqual([authenticatedDID.id])
      expect(payload.data).toEqual(testModelV1)
    })
  })

  describe('createDefinition() method', () => {
    test('posts the signed event and returns the model StreamID', async () => {
      const postEventType = jest.fn(() => randomCID())
      const ceramic = { postEventType } as unknown as CeramicClient
      const client = new ModelClient({ ceramic, did: authenticatedDID })

      const id = await client.createDefinition(testModelV1)
      expect(postEventType).toHaveBeenCalled()
      expect(id).toBeInstanceOf(StreamID)
      expect(id.type).toBe(STREAM_TYPE_ID)
    })
  })
})
