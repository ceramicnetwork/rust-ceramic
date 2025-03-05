import {
  type ClientOptions,
  type FlightSqlClient,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import { ModelClient } from '@ceramic-sdk/model-client'
import { ModelInstanceClient } from '@ceramic-sdk/model-instance-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { getAuthenticatedDID } from '@didtools/key-did'
import CeramicOneContainer, {
  waitForEventState,
  type EnvironmentOptions,
} from '../src'

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

const CONTAINER_OPTS: EnvironmentOptions = {
  containerName: 'ceramic-test-model',
  apiPort: 5222,
  flightSqlPort: 5223,
  testPort: 5223,
}

const FLIGHT_OPTIONS: ClientOptions = {
  headers: new Array(),
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: '127.0.0.1',
  port: CONTAINER_OPTS.flightSqlPort,
}

const client = new CeramicClient({
  url: `http://127.0.0.1:${CONTAINER_OPTS.apiPort}`,
})

const modelClient = new ModelClient({
  ceramic: client,
  did: authenticatedDID,
})

describe('model stream integration test', () => {
  let c1Container: CeramicOneContainer
  let flightClient: FlightSqlClient

  beforeAll(async () => {
    c1Container = await CeramicOneContainer.startContainer(CONTAINER_OPTS)
    flightClient = await createFlightSqlClient(FLIGHT_OPTIONS)
  }, 10000)

  test('create model', async () => {
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'TestModelCreation',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
        },
        additionalProperties: false,
      },
    }

    const modelStream = await modelClient.createDefinition(testModel)
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelStream.cid)

    const definition = await modelClient.getModelDefinition(modelStream)
    expect(definition).toEqual(testModel)
  })
  test('update model', async () => {
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'TestModelUpdate',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
        },
        additionalProperties: false,
      },
    }
    const testModelUpdated: ModelDefinition = {
      version: '2.0',
      name: 'UpdatedTestModel',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
          new: { type: 'number' },
        },
        additionalProperties: false,
      },
    }
    const modelStream = await modelClient.createDefinition(testModel)

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelStream.cid)

    const modelState = await modelClient.updateDefinition({
      streamID: modelStream.toString(),
      newContent: testModelUpdated,
    })

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelState.commitID.commit)

    const definition = await modelClient.getModelDefinition(modelStream)
    expect(definition).toEqual(testModelUpdated)
  })
  test('create instance for specific model version', async () => {
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'TestModelInstanceCreate',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
        },
        additionalProperties: false,
      },
    }
    const testModelUpdated: ModelDefinition = {
      version: '2.0',
      name: 'UpdatedTestModelInstanceCreate',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
          new: { type: 'number' },
        },
        additionalProperties: false,
      },
    }
    const modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID,
    })
    const modelStream = await modelClient.createDefinition(testModel)

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelStream.cid)

    const modelState = await modelClient.updateDefinition({
      streamID: modelStream.toString(),
      newContent: testModelUpdated,
    })

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelState.commitID.commit)

    // Create model instance that references the updated model version
    const documentStream = await modelInstanceClient.createInstance({
      model: modelStream,
      modelVersion: modelState.commitID.commit,
      content: { test: 'hello', new: 42 },
      shouldIndex: true,
    })
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, documentStream.commit)

    const currentState = await modelInstanceClient.getDocumentState(
      documentStream.baseID,
    )
    expect(currentState.content).toEqual({ test: 'hello', new: 42 })
  })
  test('update instance for specific model version', async () => {
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'TestModelInstanceUpdate',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
        },
        additionalProperties: false,
      },
    }
    const testModelUpdated: ModelDefinition = {
      version: '2.0',
      name: 'UpdatedTestModelInstanceUpdate',
      description: 'List Test model',
      accountRelation: { type: 'list' },
      interface: false,
      implements: [],
      schema: {
        type: 'object',
        properties: {
          test: { type: 'string', maxLength: 10 },
          new: { type: 'number' },
        },
        additionalProperties: false,
      },
    }
    const modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID,
    })
    const modelStream = await modelClient.createDefinition(testModel)

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelStream.cid)

    const modelState = await modelClient.updateDefinition({
      streamID: modelStream.toString(),
      newContent: testModelUpdated,
    })

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelState.commitID.commit)

    // Create model instance that references the updated model version
    const documentStream = await modelInstanceClient.createInstance({
      model: modelStream,
      content: { test: 'hello' },
      shouldIndex: true,
    })
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, documentStream.commit)

    // update the document
    const updatedState = await modelInstanceClient.updateDocument({
      streamID: documentStream.baseID.toString(),
      modelVersion: modelState.commitID.commit,
      newContent: { test: 'world', new: 42 },
      shouldIndex: true,
    })
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, updatedState.commitID.commit)

    const currentState = await modelInstanceClient.getDocumentState(
      documentStream.baseID,
    )
    expect(currentState.content).toEqual({ test: 'world', new: 42 })
  })
  afterAll(async () => {
    await c1Container.teardown()
  })
})
