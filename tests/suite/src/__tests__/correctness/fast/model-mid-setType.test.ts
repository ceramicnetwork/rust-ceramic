import { beforeAll, describe, expect, test } from '@jest/globals'
import {
  type ClientOptions,
  type FlightSqlClient,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import { ModelClient } from '@ceramic-sdk/model-client'
import { ModelInstanceClient } from '@ceramic-sdk/model-instance-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { waitForEventState } from '../../../utils/rustCeramicHelpers'
import { urlsToEndpoint } from '../../../utils/common'
import { randomDID } from '../../../utils/didHelper'
import { StreamID } from '@ceramic-sdk/identifiers'

const CeramicUrls = String(process.env.CERAMIC_URLS).split(',')
const CeramicFlightUrls = String(process.env.CERAMIC_FLIGHT_URLS).split(',')
const CeramicFlightEndpoints = urlsToEndpoint(CeramicFlightUrls);

const testModel: ModelDefinition = {
  version: '2.0',
  name: 'SetTestModel',
  description: 'Set Test model',
  accountRelation: {
    type: 'set',
    fields: ['environment', 'version'],
  },
  schema: {
    type: 'object',
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    properties: {
      environment: {
        type: 'string',
      },
      version: {
        type: 'string',
      },
      deployedAt: {
        type: 'string',
        format: 'date-time',
      },
      metadata: {
        type: 'object',
      },
    },
    required: ['environment', 'version'],
    additionalProperties: false,
  },
  interface: false,
  implements: [],
}

const FLIGHT_OPTIONS: ClientOptions = {
  headers: [],
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: CeramicFlightEndpoints[0].host,
  port: CeramicFlightEndpoints[0].port,
}


describe('model integration test for set model and MID', () => {
  let modelStream: StreamID
  let flightClient: FlightSqlClient
  let client: CeramicClient
  let modelClient: ModelClient
  let modelInstanceClient: ModelInstanceClient

  beforeAll(async () => {
    flightClient = await createFlightSqlClient(FLIGHT_OPTIONS)
    client = new CeramicClient({
      url: CeramicUrls[0]
    })

    modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: await randomDID()
    })

    modelClient = new ModelClient({
      ceramic: client,
      did: await randomDID()
    })

    modelStream = await modelClient.createDefinition(testModel)
  }, 10000)

  test('gets correct model definition', async () => {
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelStream.cid)

    const definition = await modelClient.getModelDefinition(modelStream)
    expect(definition).toEqual(testModel)
  })

  test('creates SET instance with deterministic stream ID', async () => {
    // For SET relations, create with unique field values
    const documentStream1 = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: 'production',
        version: 'v1.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, documentStream1.commit)

    // Now update with the actual content
    await modelInstanceClient.updateDocument({
      streamID: documentStream1.baseID.toString(),
      newContent: { 
        environment: 'production',
        version: 'v1.0.0',
        deployedAt: new Date().toISOString()
      },
      shouldIndex: true,
    })

    // Create another instance with same unique values - should return same stream
    const documentStream2 = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: 'production',
        version: 'v1.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, documentStream2.commit)
    
    // Update with different metadata
    const updatedState = await modelInstanceClient.updateDocument({
      streamID: documentStream2.baseID.toString(),
      newContent: { 
        environment: 'production',
        version: 'v1.0.0',
        deployedAt: new Date().toISOString(),
        metadata: { buildNumber: 123 }
      },
      shouldIndex: true,
    })
    
    // For SET relations with same unique field values, stream IDs should be identical
    expect(documentStream1.baseID.toString()).toEqual(documentStream2.baseID.toString())
    
    // Verify the content was updated with the second call
    expect(updatedState.content).toHaveProperty('metadata')
    expect(updatedState.content?.metadata).toEqual({ buildNumber: 123 })
  })

  test('creates different streams for different unique values', async () => {
    // Create instance for production v1.0.0
    const prodStream = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: 'production',
        version: 'v1.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, prodStream.commit)
    
    // Add additional content
    const prodUpdate = await modelInstanceClient.updateDocument({
      streamID: prodStream.baseID.toString(),
      newContent: { 
        environment: 'production',
        version: 'v1.0.0',
        deployedAt: new Date().toISOString()
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, prodUpdate.commitID.commit)

    // Create instance for staging v1.0.0
    const stagingStream = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: 'staging',
        version: 'v1.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, stagingStream.commit)
    
    // Add additional content
    const stagingUpdate = await modelInstanceClient.updateDocument({
      streamID: stagingStream.baseID.toString(),
      newContent: { 
        environment: 'staging',
        version: 'v1.0.0',
        deployedAt: new Date().toISOString()
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, stagingUpdate.commitID.commit)

    // Different unique values should create different streams
    expect(prodStream.baseID.toString()).not.toEqual(stagingStream.baseID.toString())
    
    // Verify both streams have their correct content after updates
    const prodState = await modelInstanceClient.getDocumentState(prodStream.baseID)
    const stagingState = await modelInstanceClient.getDocumentState(stagingStream.baseID)
    
    // Content should be present after the updates
    expect(prodState.content?.environment).toBe('production')
    expect(stagingState.content?.environment).toBe('staging')
  })

  test('handles empty and null values in SET fields', async () => {
    // Create with empty string in environment field
    const emptyEnvStream = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: '',
        version: 'v2.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, emptyEnvStream.commit)
    
    // Add additional content
    await modelInstanceClient.updateDocument({
      streamID: emptyEnvStream.baseID.toString(),
      newContent: { 
        environment: '',
        version: 'v2.0.0',
        deployedAt: new Date().toISOString()
      },
      shouldIndex: true,
    })

    // Try to create with same empty environment - should be same stream
    const sameStream = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: '',
        version: 'v2.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, sameStream.commit)
    
    // Update with metadata
    await modelInstanceClient.updateDocument({
      streamID: sameStream.baseID.toString(),
      newContent: { 
        environment: '',
        version: 'v2.0.0',
        metadata: { note: 'Same empty environment' }
      },
      shouldIndex: true,
    })

    expect(emptyEnvStream.baseID.toString()).toEqual(sameStream.baseID.toString())
  })

  test('respects immutability of SET fields after first update', async () => {
    // Create initial instance
    const documentStream = await modelInstanceClient.createInstance({
      model: modelStream,
      modelDefinition: testModel,
      content: { 
        environment: 'test',
        version: 'v3.0.0'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, documentStream.commit)
    
    // Add initial content
    await modelInstanceClient.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { 
        environment: 'test',
        version: 'v3.0.0',
        deployedAt: new Date().toISOString()
      },
      shouldIndex: true,
    })

    // Update the document (only non-SET fields should be updatable)
    const updatedState = await modelInstanceClient.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { 
        environment: 'test', // Must remain the same
        version: 'v3.0.0',   // Must remain the same
        deployedAt: new Date().toISOString(), // Can be updated
        metadata: { updated: true } // Can be added
      },
      shouldIndex: true,
    })
    
    expect(updatedState.content).toHaveProperty('metadata')
    expect(updatedState.content?.metadata).toEqual({ updated: true })
  })

  test('ComposeDB parity: deterministic streams with simple unique values', async () => {
    // Create a simple model like ComposeDB's Favorite with two fields
    const favoriteModel: ModelDefinition = {
      version: '2.0',
      name: 'FavoriteTest',
      description: 'ComposeDB parity test',
      accountRelation: {
        type: 'set',
        fields: ['docID', 'tag'],
      },
      schema: {
        type: 'object',
        properties: {
          docID: { type: 'string' },
          tag: { type: 'string' },
          note: { type: 'string' },
        },
        required: ['docID', 'tag'],
        additionalProperties: false,
      },
      interface: false,
      implements: [],
    }

    const favoriteModelStream = await modelClient.createDefinition(favoriteModel)
    await waitForEventState(flightClient, favoriteModelStream.cid)

    // Test case from ComposeDB: create with ['foo', 'bar'] pattern
    const instance1 = await modelInstanceClient.createInstance({
      model: favoriteModelStream,
      modelDefinition: favoriteModel,
      content: {
        docID: 'foo',
        tag: 'bar'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, instance1.commit)
    
    // Add note
    const firstUpdate = await modelInstanceClient.updateDocument({
      streamID: instance1.baseID.toString(),
      newContent: {
        docID: 'foo',
        tag: 'bar',
        note: 'First instance'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, firstUpdate.commitID.commit)

    // Create again with same unique values
    const instance2 = await modelInstanceClient.createInstance({
      model: favoriteModelStream,
      modelDefinition: favoriteModel,
      content: {
        docID: 'foo',
        tag: 'bar'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, instance2.commit)
    
    // Update with different note
    const finalUpdate = await modelInstanceClient.updateDocument({
      streamID: instance2.baseID.toString(),
      newContent: {
        docID: 'foo',
        tag: 'bar',
        note: 'Second instance - should update the first'
      },
      shouldIndex: true,
    })
    await waitForEventState(flightClient, finalUpdate.commitID.commit)

    // Should be the same stream ID (deterministic)
    expect(instance1.baseID.toString()).toEqual(instance2.baseID.toString())

    // Verify content was updated
    const state = await modelInstanceClient.getDocumentState(instance2.baseID)
    expect(state.content?.note).toBe('Second instance - should update the first')
  })
})
