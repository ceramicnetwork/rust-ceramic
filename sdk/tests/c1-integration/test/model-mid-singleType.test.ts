import { randomBytes } from 'node:crypto'
import {
  type ClientOptions,
  type FlightSqlClient,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import type { CommitID, StreamID } from '@ceramic-sdk/identifiers'
import { ModelClient } from '@ceramic-sdk/model-client'
import { ModelInstanceClient } from '@ceramic-sdk/model-instance-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { getAuthenticatedDID } from '@didtools/key-did'
import type { DID } from 'dids'
import CeramicOneContainer, {
  waitForEventState,
  type EnvironmentOptions,
} from '../src'

const testModel: ModelDefinition = {
  version: '2.0',
  name: 'SingleTestModel',
  description: 'Single Test model',
  accountRelation: { type: 'single' },
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

const CONTAINER_OPTS: EnvironmentOptions = {
  containerName: 'ceramic-test-model-MID-list',
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

describe('model integration test for list model and MID', () => {
  let c1Container: CeramicOneContainer
  let modelStream: StreamID
  let flightClient: FlightSqlClient

  beforeAll(async () => {
    c1Container = await CeramicOneContainer.startContainer(CONTAINER_OPTS)
    flightClient = await createFlightSqlClient(FLIGHT_OPTIONS)
    const authenticatedDID = await randomDID()
    const modelClient = new ModelClient({
      ceramic: client,
      did: authenticatedDID,
    })
    modelStream = await modelClient.createDefinition(testModel)
  }, 10000)

  test('gets model with account relation single', async () => {
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, modelStream.cid)
    const modelClient = new ModelClient({ ceramic: client })
    const definition = await modelClient.getModelDefinition(modelStream)
    expect(definition).toEqual(testModel)
  })
  test('creates singleton and obtains correct state', async () => {
    const authenticatedDID = await randomDID()
    const modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID,
    })
    const documentStream = await modelInstanceClient.createSingleton({
      model: modelStream,
      controller: authenticatedDID.id,
    })

    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, documentStream.commit)

    const currentState = await modelInstanceClient.getDocumentState(
      documentStream.baseID,
    )
    expect(currentState.content).toEqual(null)
  })
  test('updates document and obtains correct state', async () => {
    const authenticatedDID = await randomDID()
    const modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID,
    })
    const documentStream = await modelInstanceClient.createSingleton({
      model: modelStream,
      controller: authenticatedDID.id,
    })
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, documentStream.commit)
    // update the document
    const updatedState = await modelInstanceClient.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { test: 'hello' },
      shouldIndex: true,
    })
    expect(updatedState.content).toEqual({ test: 'hello' })
  })
  test('creates singleton twice and obtains identical correct state', async () => {
    const authenticatedDID = await randomDID()
    const modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID,
    })
    const documentStream1 = await modelInstanceClient.createSingleton({
      model: modelStream,
      controller: authenticatedDID.id,
    })
    const documentStream2 = await modelInstanceClient.createSingleton({
      model: modelStream,
      controller: authenticatedDID.id,
    })
    expect(documentStream1.baseID).toEqual(documentStream2.baseID)
  })
  test('creates singleton twice with different controllers and obtains different state', async () => {
    const authenticatedDID1 = await randomDID()
    const authenticatedDID2 = await randomDID()
    const modelInstanceClient1 = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID1,
    })
    const modelInstanceClient2 = new ModelInstanceClient({
      ceramic: client,
      did: authenticatedDID2,
    })
    const documentStream1 = await modelInstanceClient1.createSingleton({
      model: modelStream,
      controller: authenticatedDID1.id,
    })
    const documentStream2 = await modelInstanceClient2.createSingleton({
      model: modelStream,
      controller: authenticatedDID2.id,
    })
    expect(documentStream1.baseID).not.toEqual(documentStream2.baseID)
  })
  afterAll(async () => {
    await c1Container.teardown()
  })

  async function randomDID(): Promise<DID> {
    return await getAuthenticatedDID(randomBytes(32))
  }
})
