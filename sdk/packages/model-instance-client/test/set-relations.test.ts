import { InitEventPayload, SignedEvent } from '@ceramic-sdk/events'
import type { CeramicClient } from '@ceramic-sdk/http-client'
import { StreamID } from '@ceramic-sdk/identifiers'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import type { StreamState } from '@ceramicnetwork/common'
import { getAuthenticatedDID } from '@didtools/key-did'
import { jest } from '@jest/globals'
import type { DID } from 'dids'
import { bases } from 'multiformats/basics'
import { ModelInstanceClient } from '../src'

describe('ModelInstanceClient SET Relations', () => {
  let client: ModelInstanceClient
  let mockCeramic: CeramicClient
  let authenticatedDID: DID

  // Helper to create multibase encoded data
  const encodeMultibase = (data: unknown): string => {
    const json = typeof data === 'string' ? data : JSON.stringify(data)
    const bytes = new TextEncoder().encode(json)
    return bases.base64url.encode(bytes)
  }

  // Helper to encode StreamID for dimensions
  const encodeStreamID = (streamId: StreamID): string => {
    return bases.base64url.encode(streamId.bytes)
  }

  beforeEach(async () => {
    authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

    mockCeramic = {
      getStreamState: jest.fn(),
      postEventType: jest.fn(),
      getVersion: jest.fn().mockResolvedValue({ version: '0.55.0' }),
      api: {
        GET: jest.fn().mockResolvedValue({
          data: {
            id: 'kjzl6kcym7w8y8xqxtdnzh6x6ehb2dkcqc37hbmm5fhc6xzpv9m1muon9y5avyn',
            data: encodeMultibase({ content: null }),
            event_cid:
              'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a',
            dimensions: {
              model: encodeStreamID(
                StreamID.fromString(
                  'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
                ),
              ),
            },
            controller: authenticatedDID.id,
          },
          error: null,
        }),
      },
    } as CeramicClient

    client = new ModelInstanceClient({
      ceramic: mockCeramic,
      did: authenticatedDID,
    })
  })

  describe('createSingleton method with unique values', () => {
    test('should create deterministic instance with unique values', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      mockCeramic.postEventType.mockResolvedValueOnce(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      // Create deterministic instance with explicit unique values
      const uniqueValue = new TextEncoder().encode('production|v1.2.3')
      await client.createSingleton({
        model: modelId,
        controller: authenticatedDID,
        uniqueValue,
      })

      // Verify the unique value was passed correctly
      expect(mockCeramic.postEventType).toHaveBeenCalled()
      const [eventType, payload] = mockCeramic.postEventType.mock.calls[0]

      // Should use InitEventPayload (deterministic)
      expect(eventType).toBe(InitEventPayload)

      // Check the unique value
      const uniqueBytes = (payload as InitEventPayload).header.unique
      const uniqueString = new TextDecoder().decode(uniqueBytes)
      expect(uniqueString).toBe('production|v1.2.3')
    })

    test('should handle empty unique values', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      const uniqueValue = new TextEncoder().encode('|tag')
      await client.createSingleton({
        model: modelId,
        controller: authenticatedDID,
        uniqueValue,
      })

      const [, payload] = mockCeramic.postEventType.mock.calls[0]
      const uniqueString = new TextDecoder().decode(payload.header.unique)
      expect(uniqueString).toBe('|tag')
    })
  })

  describe('createInstance method', () => {
    test('should default to LIST relation when modelDefinition is not provided', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      // When no modelDefinition is provided, it should default to LIST relation
      await client.createInstance({
        model: modelId,
        content: { test: 'data' },
      })

      // Should use SignedEvent for LIST relations
      expect(mockCeramic.postEventType).toHaveBeenCalledWith(
        SignedEvent,
        expect.any(Object),
      )
    })

    test('should handle SET relation with automatic unique extraction', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'TestModel',
        accountRelation: {
          type: 'set',
          fields: ['environment', 'version'],
        },
        interface: false,
        implements: [],
        schema: {
          type: 'object',
          properties: {
            environment: { type: 'string' },
            version: { type: 'string' },
            data: { type: 'string' },
          },
        },
      }

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      const content = {
        environment: 'production',
        version: 'v1.2.3',
        data: 'some data',
      }

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content,
        shouldIndex: true,
      })

      const [eventType, payload] = mockCeramic.postEventType.mock.calls[0]
      expect(eventType).toBe(InitEventPayload)

      const uniqueString = new TextDecoder().decode(payload.header.unique)
      expect(uniqueString).toBe('production|v1.2.3')
    })

    test('should handle missing field values in SET relation', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'TestModel',
        accountRelation: {
          type: 'set',
          fields: ['field1', 'field2', 'field3'],
        },
        interface: false,
        implements: [],
        schema: {},
      }

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      const content = {
        field1: 'value1',
        field3: 'value3',
        // field2 is missing
      }

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content,
      })

      const [, payload] = mockCeramic.postEventType.mock.calls[0]
      const uniqueString = new TextDecoder().decode(payload.header.unique)
      expect(uniqueString).toBe('value1||value3')
    })

    test('should convert non-string values correctly', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'TestModel',
        accountRelation: {
          type: 'set',
          fields: ['boolField', 'numField', 'strField'],
        },
        interface: false,
        implements: [],
        schema: {},
      }

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      const content = {
        boolField: true,
        numField: 42,
        strField: 'hello',
      }

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content,
      })

      const [, payload] = mockCeramic.postEventType.mock.calls[0]
      const uniqueString = new TextDecoder().decode(payload.header.unique)
      expect(uniqueString).toBe('true|42|hello')
    })

    test('should handle LIST relation', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'ListModel',
        accountRelation: { type: 'list' },
        interface: false,
        implements: [],
        schema: {},
      }

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content: { data: 'test' },
      })

      // Should use SignedEvent for LIST relations
      expect(mockCeramic.postEventType).toHaveBeenCalledWith(
        SignedEvent,
        expect.any(Object),
      )
    })

    test('should handle SINGLE relation', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'SingleModel',
        accountRelation: { type: 'single' },
        interface: false,
        implements: [],
        schema: {},
      }

      mockCeramic.postEventType.mockResolvedValueOnce(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
      })

      // Should create singleton without unique value
      const [, payload] = mockCeramic.postEventType.mock.calls[0]
      expect(payload.header.unique).toBeUndefined()
    })
  })

  describe('Core logic validation', () => {
    test('createInstance correctly calculates unique values for SET relations', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'TestModel',
        accountRelation: {
          type: 'set',
          fields: ['field1', 'field2', 'field3'],
        },
        interface: false,
        implements: [],
        schema: {},
      }

      const content = {
        field1: 'field1Value',
        field2: 'field2Value',
        field3: 'field3Value',
        extra: 'data',
      }

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content,
      })

      // Verify InitEventPayload was posted with correct unique value
      expect(mockCeramic.postEventType).toHaveBeenCalledWith(
        InitEventPayload,
        expect.objectContaining({
          header: expect.objectContaining({
            unique: new TextEncoder().encode(
              'field1Value|field2Value|field3Value',
            ),
          }),
        }),
      )
    })

    test('createInstance extracts unique values from content for SET relations', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'TestModel',
        accountRelation: {
          type: 'set',
          fields: ['color', 'size', 'category'],
        },
        interface: false,
        implements: [],
        schema: {},
      }

      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content: {
          color: 'red',
          size: 42,
          category: null,
          description: 'This field is not in the unique fields',
        },
      })

      // Verify unique value was correctly calculated
      const [, payload] = mockCeramic.postEventType.mock.calls[0]
      const uniqueString = new TextDecoder().decode(payload.header.unique)
      expect(uniqueString).toBe('red|42|') // color as-is, size converted to string, category as empty string
    })
  })

  describe('ComposeDB parity tests', () => {
    test('should create deterministic stream for SET relation like ComposeDB', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      const modelDef: ModelDefinition = {
        version: '2.0',
        name: 'TestModel',
        accountRelation: {
          type: 'set',
          fields: ['field1', 'field2'],
        },
        interface: false,
        implements: [],
        schema: {},
      }

      // Mock responses
      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )

      // Test case from ComposeDB: ['foo', 'bar'] unique values
      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content: { field1: 'foo', field2: 'bar' },
      })

      // Create again with same unique values
      await client.createInstance({
        model: modelId,
        modelDefinition: modelDef,
        content: { field1: 'foo', field2: 'bar', extra: 'data' },
      })

      // Both should create deterministic init events with same unique value
      const [, payload1] = mockCeramic.postEventType.mock.calls[0]
      const [, payload2] = mockCeramic.postEventType.mock.calls[1]

      const unique1 = new TextDecoder().decode(payload1.header.unique)
      const unique2 = new TextDecoder().decode(payload2.header.unique)

      expect(unique1).toBe('foo|bar')
      expect(unique2).toBe('foo|bar')
    })

    test('should handle SET relation fields like Favorite model from ComposeDB', async () => {
      const modelId = StreamID.fromString(
        'kjzl6hvfrbw6c5ynhkxyaqzyllij9mpv9lrnce35d1jlhfdd6mhkp09xcywvt9k',
      )

      // Favorite model from ComposeDB has accountRelationFields: ["docID", "tag"]
      const favoriteModel: ModelDefinition = {
        version: '2.0',
        name: 'Favorite',
        description: 'A set of favorite documents',
        accountRelation: {
          type: 'set',
          fields: ['docID', 'tag'],
        },
        interface: false,
        implements: [],
        schema: {
          type: 'object',
          properties: {
            docID: { type: 'string' },
            tag: { type: 'string', minLength: 2, maxLength: 20 },
            note: { type: 'string', maxLength: 500 },
          },
          required: ['docID', 'tag'],
        },
      }

      mockCeramic.postEventType.mockResolvedValue(
        'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a' as string,
      )
      mockCeramic.getStreamState.mockResolvedValue({
        id: 'kjzl6kcym7w8y8xqxtdnzh6x6ehb2dkcqc37hbmm5fhc6xzpv9m1muon9y5avyn',
        data: encodeMultibase({ content: null }),
        event_cid:
          'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a',
        dimensions: { model: encodeStreamID(modelId) },
        controller: authenticatedDID.id,
      } as StreamState)

      // Create favorite instance
      await client.createInstance({
        model: modelId,
        modelDefinition: favoriteModel,
        content: {
          docID:
            'kjzl6kcym7w8y9zdwz9q2r5bx5srfhdtjvs7ouq2kimk9od0f96v8kp29e37efu',
          tag: 'important',
          note: 'This is a very important document',
        },
      })

      const [, payload] = mockCeramic.postEventType.mock.calls[0]
      const uniqueString = new TextDecoder().decode(payload.header.unique)
      expect(uniqueString).toBe(
        'kjzl6kcym7w8y9zdwz9q2r5bx5srfhdtjvs7ouq2kimk9od0f96v8kp29e37efu|important',
      )
    })
  })
})
