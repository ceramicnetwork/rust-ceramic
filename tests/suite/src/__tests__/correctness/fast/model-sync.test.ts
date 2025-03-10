import { beforeAll, describe, expect, test } from '@jest/globals'
import {
  type ClientOptions,
  type FlightSqlClient,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import type { StreamID } from '@ceramic-sdk/identifiers'
import { ModelClient } from '@ceramic-sdk/model-client'
import { ModelInstanceClient } from '@ceramic-sdk/model-instance-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { randomDID } from '../../../utils/didHelper'
import { registerInterest, registerInterestMetaModel, waitForEventState } from '../../../utils/rustCeramicHelpers'
import { urlsToEndpoint } from '../../../utils/common'

const CeramicUrls = String(process.env.CERAMIC_URLS).split(',')
const CeramicFlightUrls = String(process.env.CERAMIC_FLIGHT_URLS).split(',')
const CeramicFlightEndpoints = urlsToEndpoint(CeramicFlightUrls);

const testModel: ModelDefinition = {
  version: '2.0',
  name: 'ListTestModel',
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

const FLIGHT_OPTIONS_0: ClientOptions = {
  headers: new Array(),
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: CeramicFlightEndpoints[0].host,
  port: CeramicFlightEndpoints[0].port,
}
const FLIGHT_OPTIONS_1: ClientOptions = {
  headers: new Array(),
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: CeramicFlightEndpoints[1].host,
  port: CeramicFlightEndpoints[1].port,
}


describe('model integration test for list model and MID', () => {
  let modelStream: StreamID
  let flightClient0: FlightSqlClient
  let client0: CeramicClient
  let modelClient0: ModelClient
  let modelInstanceClient0: ModelInstanceClient
  let flightClient1: FlightSqlClient
  let client1: CeramicClient
  let modelClient1: ModelClient
  let modelInstanceClient1: ModelInstanceClient

  beforeAll(async () => {
    if (!CeramicUrls[1]) {
      throw new Error('expect minimum 2 ceramic nodes to test syncing mids')
    }
    flightClient0 = await createFlightSqlClient(FLIGHT_OPTIONS_0)
    client0 = new CeramicClient({
      url: CeramicUrls[0]
    })

    modelInstanceClient0 = new ModelInstanceClient({
      ceramic: client0,
      did: await randomDID(),
    })

    modelClient0 = new ModelClient({
      ceramic: client0,
      did: await randomDID(),
    })

    flightClient1 = await createFlightSqlClient(FLIGHT_OPTIONS_1)
    client1 = new CeramicClient({
      url: CeramicUrls[1]
    })

    modelInstanceClient1 = new ModelInstanceClient({
      ceramic: client1,
      did: await randomDID(),
    })

    modelClient1 = new ModelClient({
      ceramic: client1,
      did: await randomDID(),
    })

    modelStream = await modelClient0.createDefinition(testModel)

    await registerInterestMetaModel(CeramicUrls[0])
    await registerInterestMetaModel(CeramicUrls[1])

    await registerInterest(CeramicUrls[0], modelStream)
    await registerInterest(CeramicUrls[1], modelStream)
  }, 10000)

  test('models and instances sync between nodes', async () => {
    await waitForEventState(flightClient0, modelStream.cid)
    await waitForEventState(flightClient1, modelStream.cid)

    const definition0 = await modelClient0.getModelDefinition(modelStream)
    expect(definition0).toEqual(testModel)

    const definition1 = await modelClient1.getModelDefinition(modelStream)
    expect(definition1).toEqual(testModel)

    const documentStream = await modelInstanceClient0.createInstance({
      model: modelStream,
      content: { test: 'hello' },
      shouldIndex: true,
    })
    await waitForEventState(flightClient0, documentStream.commit)
    // update the document
    const updatedState0 = await modelInstanceClient0.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { test: 'world' },
      shouldIndex: true,
    })
    expect(updatedState0.content).toEqual({ test: 'world' })

    // Wait to observe the update on the second node
    await waitForEventState(flightClient1, updatedState0.commitID.commit)
    const updatedState1 = await modelInstanceClient1.getDocumentState(documentStream.baseID.toString())
    expect(updatedState1.content).toEqual({ test: 'world' })
  })
})

