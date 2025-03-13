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

interface Clients {
  url: string,
  ceramic: CeramicClient,
  flight: FlightSqlClient,
  model: ModelClient,
  modelInstance: ModelInstanceClient,
}


describe('integration test for syncing models and mids', () => {
  let modelStream: StreamID
  let clients: Clients[]

  beforeAll(async () => {
    if (!CeramicUrls[1]) {
      throw new Error('expect minimum 2 ceramic nodes to test syncing mids')
    }
    // Use the same did for all clients so any client can make an update
    let did = await randomDID()
    clients = []
    for (const [index, url] of CeramicUrls.entries()) {
      const flight_options: ClientOptions = {
        headers: new Array(),
        username: undefined,
        password: undefined,
        token: undefined,
        tls: false,
        host: CeramicFlightEndpoints[index].host,
        port: CeramicFlightEndpoints[index].port,
      }
      const flight = await createFlightSqlClient(flight_options)
      const ceramic = new CeramicClient({ url })
      const model = new ModelClient({
        ceramic: ceramic,
        did,
      })
      const modelInstance = new ModelInstanceClient({
        ceramic: ceramic,
        did,
      })
      clients.push({
        url,
        ceramic,
        flight,
        model,
        modelInstance,
      })
    }

    modelStream = await clients[0].model.createDefinition(testModel)

    for (const client of clients) {
      await registerInterestMetaModel(client.url)
      await registerInterest(client.url, modelStream)
    }
  }, 10000)

  test('models and instances sync between nodes', async () => {
    for (const client of clients) {
      await waitForEventState(client.flight, modelStream.cid)
    }
    for (const client of clients) {
      const definition = await client.model.getModelDefinition(modelStream)
      expect(definition).toEqual(testModel)
    }

    const documentStream = await clients[0].modelInstance.createInstance({
      model: modelStream,
      content: { test: 'hello' },
      shouldIndex: true,
    })
    await waitForEventState(clients[0].flight, documentStream.commit)
    // update the document
    const updatedState0 = await clients[0].modelInstance.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { test: 'world' },
      shouldIndex: true,
    })
    expect(updatedState0.content).toEqual({ test: 'world' })

    // Wait to observe the update on the all the nodes
    for (const client of clients) {
      await waitForEventState(client.flight, updatedState0.commitID.commit)
      const updatedState = await client.modelInstance.getDocumentState(documentStream.baseID.toString())
      expect(updatedState.content).toEqual({ test: 'world' })
    }

    // Make another update on the last node and watch it sync to the other nodes
    const updatedStateN = await clients[clients.length - 1].modelInstance.updateDocument({
      streamID: documentStream.baseID.toString(),
      newContent: { test: 'world!' },
      shouldIndex: true,
    })
    expect(updatedStateN.content).toEqual({ test: 'world!' })

    // Wait to observe the update on the all the nodes
    for (const client of clients) {
      await waitForEventState(client.flight, updatedStateN.commitID.commit)
      const updatedState = await client.modelInstance.getDocumentState(documentStream.baseID.toString())
      expect(updatedState.content).toEqual({ test: 'world!' })
    }
  })
})

