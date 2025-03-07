import { beforeAll, describe, expect, test } from '@jest/globals'
import {
  type ClientOptions,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import { StreamID } from '@ceramic-sdk/identifiers'
import { ModelClient } from '@ceramic-sdk/model-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { StreamClient } from '@ceramic-sdk/stream-client'
import { urlsToEndpoint } from '../../../utils/common'
import { waitForEventState } from '../../../utils/rustCeramicHelpers'
import { randomDID } from '../../../utils/didHelper'
import { ModelInstanceClient } from '@ceramic-sdk/model-instance-client'

const CeramicUrls = String(process.env.CERAMIC_URLS).split(',')
const CeramicFlightUrls = String(process.env.CERAMIC_FLIGHT_URLS).split(',')
const CeramicFlightEndpoints = urlsToEndpoint(CeramicFlightUrls);


const OPTIONS: ClientOptions = {
  headers: new Array(),
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: CeramicFlightEndpoints[0].host,
  port: CeramicFlightEndpoints[0].port,
}

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
describe('stream client', () => {
  const ceramicClient = new CeramicClient({
    url: CeramicUrls[0]
  })
  let streamId: StreamID

  beforeAll(async () => {
    let authenticatedDID = await randomDID()
    const modelClient = new ModelClient({
      ceramic: ceramicClient,
      did: authenticatedDID,
    })
    const modelInstanceClient = new ModelInstanceClient({
      ceramic: ceramicClient,
      did: authenticatedDID,
    })
    const model = await modelClient.createDefinition(testModel)
    const documentStream = await modelInstanceClient.createInstance({
      model: model,
      content: { test: 'hello' },
      shouldIndex: true,
    })

    // obtain the stream ID by waiting for the event state to be populated
    const flightClient = await createFlightSqlClient(OPTIONS)
    await waitForEventState(flightClient, documentStream.commit)
    streamId = documentStream.baseID
    console.log('stream id', streamId)
  }, 20000)

  test('gets a stream', async () => {
    const client = new StreamClient({ ceramic: ceramicClient })
    const streamState = await client.getStreamState(streamId)
    expect(streamState).toBeDefined()
    expect(streamState.id.toString()).toEqual(streamId.toString())
  }, 10000)
})
