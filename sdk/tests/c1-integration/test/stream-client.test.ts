import { InitEventPayload, SignedEvent, signEvent } from '@ceramic-sdk/events'
import {
  type ClientOptions,
  type FlightSqlClient,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import { StreamID } from '@ceramic-sdk/identifiers'
import { ModelClient } from '@ceramic-sdk/model-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { StreamClient } from '@ceramic-sdk/stream-client'
import { asDIDString } from '@didtools/codecs'
import { getAuthenticatedDID } from '@didtools/key-did'
import { tableFromIPC } from 'apache-arrow'
import { CID } from 'multiformats'
import type { EnvironmentOptions } from '../src'
import CeramicOneContainer from '../src'

const CONTAINER_OPTS: EnvironmentOptions = {
  containerName: 'ceramic-test-stream-client',
  apiPort: 5222,
  flightSqlPort: 5223,
  testPort: 5223,
}

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

const OPTIONS: ClientOptions = {
  headers: new Array(),
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: '127.0.0.1',
  port: CONTAINER_OPTS.flightSqlPort,
}

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
describe('stream client', () => {
  let c1Container: CeramicOneContainer
  const ceramicClient = new CeramicClient({
    url: `http://127.0.0.1:${CONTAINER_OPTS.apiPort}`,
  })
  let streamId: StreamID
  let cid: CID
  beforeAll(async () => {
    c1Container = await CeramicOneContainer.startContainer(CONTAINER_OPTS)
    const client = new CeramicClient({
      url: `http://127.0.0.1:${CONTAINER_OPTS.apiPort}`,
    })

    // create a new event
    const modelClient = new ModelClient({
      ceramic: client,
      did: authenticatedDID,
    })
    const model = await modelClient.createDefinition(testModel)
    const eventPayload: InitEventPayload = {
      data: {
        test: 'test message',
      },
      header: {
        controllers: [asDIDString(authenticatedDID.id)],
        model,
        sep: 'model',
      },
    }
    const encodedPayload = InitEventPayload.encode(eventPayload)
    const signedEvent = await signEvent(authenticatedDID, encodedPayload)
    cid = await ceramicClient.postEventType(SignedEvent, signedEvent)

    // obtain the stream ID by waiting for the event state to be populated
    const flightClient = await createFlightSqlClient(OPTIONS)
    const buffer = await flightClient.query(
      'SELECT stream_cid FROM event_states_feed LIMIT 1',
    )
    const data = tableFromIPC(buffer)
    const row = data.get(0)
    const streamCid = CID.decode(row?.stream_cid).toString()
    streamId = new StreamID('MID', streamCid)
  }, 20000)

  test('gets a stream', async () => {
    const client = new StreamClient({ ceramic: ceramicClient })
    const streamState = await client.getStreamState(streamId)
    expect(streamState).toBeDefined()
    expect(streamState.id.toString()).toEqual(streamId.toString())
  }, 10000)

  afterAll(async () => {
    await c1Container.teardown()
  })
})
