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
    fields: ['test'],
  },
  schema: {
    type: 'object',
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    properties: {
      test: {
        type: 'string',
      },
    },
    required: ['test'],
    additionalProperties: false,
  },
  interface: false,
  implements: [],
}

const FLIGHT_OPTIONS: ClientOptions = {
  headers: new Array(),
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
  test('creates instance and obtains correct state', async () => {
    const documentStream = await modelInstanceClient.createInstance({
      model: modelStream,
      content: null,
      shouldIndex: true,
    })
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, documentStream.commit)

    const currentState = await modelInstanceClient.getDocumentState(
      documentStream.baseID,
    )
    expect(currentState.content).toBeNull()
  })
  test('updates document and obtains correct state', async () => {
    const documentStream = await modelInstanceClient.createInstance({
      model: modelStream,
      content: null,
      shouldIndex: true,
    })
    // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
    await waitForEventState(flightClient, documentStream.commit)
    // update the document
    const updatedState = await modelInstanceClient.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { test: 'world' },
      shouldIndex: true,
    })
    expect(updatedState.content).toEqual({ test: 'world' })
  })
})
