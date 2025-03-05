import { InitEventPayload, SignedEvent, signEvent } from '@ceramic-sdk/events'
import {
  type ClientOptions,
  type FlightSqlClient,
  createFlightSqlClient,
} from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import type { StreamID } from '@ceramic-sdk/identifiers'
import { ModelClient } from '@ceramic-sdk/model-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { asDIDString } from '@didtools/codecs'
import { getAuthenticatedDID } from '@didtools/key-did'
import { tableFromIPC } from 'apache-arrow'
import type { DID } from 'dids'
import { base16 } from 'multiformats/bases/base16'
import type { CID } from 'multiformats/cid'
import CeramicOneContainer from '../src'
import type { EnvironmentOptions } from '../src'

const CONTAINER_OPTS: EnvironmentOptions = {
  containerName: 'ceramic-test-flight',
  apiPort: 5222,
  flightSqlPort: 5223,
  testPort: 5223,
}

const OPTIONS: ClientOptions = {
  headers: new Array(),
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: '127.0.0.1',
  port: CONTAINER_OPTS.flightSqlPort,
}

async function getClient(): Promise<FlightSqlClient> {
  return createFlightSqlClient(OPTIONS)
}

describe('flight sql', () => {
  let c1Container: CeramicOneContainer
  const ceramicClient = new CeramicClient({
    url: `http://127.0.0.1:${CONTAINER_OPTS.apiPort}`,
  })
  let authenticatedDID: DID

  beforeAll(async () => {
    c1Container = await CeramicOneContainer.startContainer(CONTAINER_OPTS)
    authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))
  }, 20000)

  test('makes query', async () => {
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
    const modelClient = new ModelClient({
      ceramic: ceramicClient,
      did: authenticatedDID,
    })
    const stream = await modelClient.createDefinition(testModel)
    const stream_cid_hex = stream.cid.toString(base16.encoder).substring(1)

    // Intentionally not using prepared query as we are testing query.
    // However we still need to filter the query otherwise writes from other tests may get returned.
    const client = await getClient()
    const buffer = await client.query(
      `SELECT * FROM conclusion_events WHERE stream_cid = X'${stream_cid_hex}'`,
    )

    const table = tableFromIPC(buffer)
    expect(table.numRows).toBe(1)

    const row = table.get(0)
    expect(row).toBeDefined()
    expect(row?.data).toBeDefined()
    expect(row?.stream_type).toBe(2)
  })

  test('catalogs', async () => {
    const client = await getClient()
    const buffer = await client.getCatalogs()
    const data = tableFromIPC(buffer)
    const row = data.get(0)
    expect(row).toBeDefined()
  })

  test('schemas', async () => {
    const client = await getClient()
    const buffer = await client.getDbSchemas({})
    const data = tableFromIPC(buffer)
    const row = data.get(0)
    expect(row).toBeDefined()
  })

  test('tables', async () => {
    const client = await getClient()
    const withSchema = await client.getTables({ includeSchema: true })
    const noSchema = await client.getTables({ includeSchema: false })
    expect(withSchema).not.toBe(noSchema)
  })

  test('prepared query', async () => {
    // create a model streamType
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'AnotherListTestModel',
      description: 'Another List Test model',
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
    const modelClient = new ModelClient({
      ceramic: ceramicClient,
      did: authenticatedDID,
    })
    const stream = await modelClient.createDefinition(testModel)
    console.log('stream', stream)
    const client = await getClient()
    const buffer = await client.preparedQuery(
      'SELECT * FROM conclusion_events WHERE stream_cid = $stream_cid',
      new Array(['$stream_cid', stream.cid.toString(base16.encoder)]),
    )
    const table = tableFromIPC(buffer)
    expect(table.numRows).toBe(1)

    const row = table.get(0)
    expect(row).toBeDefined()
    expect(row?.data).toBeDefined()
    expect(row?.stream_type).toBe(2)
  })

  test('feed query', async () => {
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'FeedQueryModel',
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
    const modelClient = new ModelClient({
      ceramic: ceramicClient,
      did: authenticatedDID,
    })
    const model = await modelClient.createDefinition(testModel)
    console.log('model', model)
    const model_hex = base16.encode(model.bytes).substring(1)
    // biome-ignore lint/suspicious/noExplicitAny: Row type depends on the query, we do not need to construct a type for a one off test query.
    const expectEvent = (row: any | null) => {
      expect(row?.dimensions?.model).toBeDefined()
      expect(base16.encode(row?.dimensions?.model)).toBe(
        base16.encode(model.bytes),
      )
      expect(row?.stream_type).toBe(3)
      expect(row?.data).toBeDefined()
    }
    const client = await getClient()
    // Intentionally not using prepared query as we are testing query.
    // However we still need to filter the query otherwise writes from other tests may get returned.
    const query = await client.feedQuery(
      `SELECT * FROM conclusion_events_feed WHERE array_extract(map_extract(dimensions, 'model'),1) = X'${model_hex}' LIMIT 4`,
    )

    let remaining = 4
    writeNewEvent(ceramicClient, model, 'event a')
    writeNewEvent(ceramicClient, model, 'event b')
    writeNewEvent(ceramicClient, model, 'event c')
    writeNewEvent(ceramicClient, model, 'event d')
    // Concurrent with the writes expect we get the events back
    while (remaining > 0) {
      const buffer = await query.next()
      const table = tableFromIPC(buffer)
      for (const row of table) {
        expectEvent(row)
        remaining -= 1
      }
    }

    // Expect stream query to be complete
    expect(await query.next()).toBeNull()
  }, 10000)

  test('prepared feed query', async () => {
    const testModel: ModelDefinition = {
      version: '2.0',
      name: 'PreparedFeedQueryModel',
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
    const modelClient = new ModelClient({
      ceramic: ceramicClient,
      did: authenticatedDID,
    })
    const model = await modelClient.createDefinition(testModel)
    console.log('prepared model', model)
    // biome-ignore lint/suspicious/noExplicitAny: Row type depends on the query, we do not need to construct a type for a one off test query.
    const expectEvent = (row: any | null) => {
      expect(row?.dimensions?.model).toBeDefined()
      expect(base16.encode(row?.dimensions?.model)).toBe(
        base16.encode(model.bytes),
      )
      expect(row?.stream_type).toBe(3)
      expect(row?.data).toBeDefined()
    }
    const client = await getClient()
    const query = await client.preparedFeedQuery(
      `SELECT * FROM conclusion_events_feed WHERE array_extract(map_extract(dimensions, 'model'),1) = $model LIMIT 4`,
      new Array(['$model', base16.encode(model.bytes)]),
    )

    let remaining = 4
    writeNewEvent(ceramicClient, model, 'event a')
    writeNewEvent(ceramicClient, model, 'event b')
    writeNewEvent(ceramicClient, model, 'event c')
    writeNewEvent(ceramicClient, model, 'event d')
    // Concurrent with the writes expect we get the events back
    while (remaining > 0) {
      const buffer = await query.next()
      const table = tableFromIPC(buffer)
      for (const row of table) {
        expectEvent(row)
        remaining -= 1
      }
    }

    // Expect stream query to be complete
    expect(await query.next()).toBeNull()
  }, 20000)

  afterAll(async () => {
    await c1Container.teardown()
  })

  async function writeNewEvent(
    ceramicClient: CeramicClient,
    model: StreamID,
    body: string,
  ): Promise<CID> {
    const eventPayload: InitEventPayload = {
      data: { body },
      header: {
        controllers: [asDIDString(authenticatedDID.id)],
        model,
        sep: 'model',
      },
    }
    const encodedPayload = InitEventPayload.encode(eventPayload)
    const signedEvent = await signEvent(authenticatedDID, encodedPayload)
    return await ceramicClient.postEventType(SignedEvent, signedEvent)
  }
})
