import { beforeAll, describe, expect, test, jest } from '@jest/globals'
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
import { tableFromIPC } from 'apache-arrow'
import { randomDID } from '../../../utils/didHelper'
import { waitForEventState } from '../../../utils/rustCeramicHelpers'
import { urlsToEndpoint, utilities } from '../../../utils/common'

const delayMs = utilities.delayMs

const CeramicUrls = String(process.env.CERAMIC_URLS).split(',')
const CeramicFlightUrls = String(process.env.CERAMIC_FLIGHT_URLS).split(',')
const CeramicFlightEndpoints = urlsToEndpoint(CeramicFlightUrls)

const FLIGHT_OPTIONS: ClientOptions = {
  headers: [],
  username: undefined,
  password: undefined,
  token: undefined,
  tls: false,
  host: CeramicFlightEndpoints[0].host,
  port: CeramicFlightEndpoints[0].port,
}

// Self-anchoring should complete within 2 minutes (anchor interval + processing time)
const ANCHOR_TIMEOUT_MS = 2 * 60 * 1000
const POLL_INTERVAL_MS = 5000

// Expected chain ID for local Ganache network configured in basic-rust.yaml
const EXPECTED_CHAIN_ID = 'eip155:1337'

const testModel: ModelDefinition = {
  version: '2.0',
  name: 'SelfAnchorTestModel',
  description: 'Model for testing self-anchoring',
  accountRelation: { type: 'list' },
  interface: false,
  implements: [],
  schema: {
    type: 'object',
    properties: {
      value: { type: 'integer' },
    },
    additionalProperties: false,
  },
}

/**
 * Wait for an event to be anchored (chain_id populated) for a given stream.
 * Polls the event_states table for events that have chain_id set.
 */
async function waitForAnchoredEvent(
  flightClient: FlightSqlClient,
  streamCid: string,
  timeoutMs: number = ANCHOR_TIMEOUT_MS,
): Promise<{ anchored: boolean; chainId: string | null }> {
  const startTime = Date.now()
  while (Date.now() - startTime < timeoutMs) {
    try {
      // Query for events that have been anchored (chain_id is not null)
      const buffer = await flightClient.query(
        `SELECT chain_id FROM event_states
         WHERE cid_string(stream_cid) = '${streamCid.toString()}'
         AND chain_id IS NOT NULL
         LIMIT 1`,
      )

      const table = tableFromIPC(buffer)
      if (table.numRows > 0) {
        const row = table.get(0)
        const chainId = row?.chain_id as string | null
        console.log(`Found anchored event for stream with chain_id: ${chainId}`)
        return { anchored: true, chainId }
      }
    } catch (error) {
      console.log(`Query error (retrying): ${error}`)
    }

    await delayMs(POLL_INTERVAL_MS)
  }

  return { anchored: false, chainId: null }
}

/**
 * Count anchored events for a given stream.
 */
async function countAnchoredEvents(
  flightClient: FlightSqlClient,
  streamCid: string,
): Promise<number> {
  const buffer = await flightClient.query(
    `SELECT COUNT(*) as count FROM event_states
     WHERE cid_string(stream_cid) = '${streamCid}'
     AND chain_id IS NOT NULL`,
  )

  const table = tableFromIPC(buffer)
  const row = table.get(0)
  return row ? Number(row.count) : 0
}

describe('self-anchoring integration test', () => {
  jest.setTimeout(1000 * 60 * 10) // 10 minutes total test timeout

  let flightClient: FlightSqlClient
  let client: CeramicClient
  let modelClient: ModelClient
  let modelInstanceClient: ModelInstanceClient
  let modelStream: StreamID

  beforeAll(async () => {
    flightClient = await createFlightSqlClient(FLIGHT_OPTIONS)

    client = new CeramicClient({
      url: CeramicUrls[0],
    })

    modelClient = new ModelClient({
      ceramic: client,
      did: await randomDID(),
    })

    modelInstanceClient = new ModelInstanceClient({
      ceramic: client,
      did: await randomDID(),
    })

    // Create test model
    modelStream = await modelClient.createDefinition(testModel)
    await waitForEventState(flightClient, modelStream.cid)
    console.log(`Created test model: ${modelStream.toString()}`)
  }, 30000)

  test(
    'document creation triggers self-anchoring',
    async () => {
      // Create a document
      console.log('Creating document...')
      const documentStream = await modelInstanceClient.createInstance({
        model: modelStream,
        content: { value: 42 },
        shouldIndex: true,
      })

      // Wait for the init event to be processed
      await waitForEventState(flightClient, documentStream.commit)
      console.log(`Document created: ${documentStream.baseID.toString()}`)

      // Get the stream CID for querying
      const streamCid = documentStream.baseID.cid.toString()
      console.log(`Waiting for anchored event for stream CID: ${streamCid}`)

      // Wait for anchoring to complete
      const result = await waitForAnchoredEvent(flightClient, streamCid, ANCHOR_TIMEOUT_MS)

      expect(result.anchored).toBe(true)
      expect(result.chainId).toBe(EXPECTED_CHAIN_ID)
      console.log(
        `Document successfully anchored via self-anchoring with chain_id: ${result.chainId}`,
      )
    },
    ANCHOR_TIMEOUT_MS + 30000,
  )

  test(
    'document update triggers self-anchoring',
    async () => {
      // Create a document first
      console.log('Creating document...')
      const documentStream = await modelInstanceClient.createInstance({
        model: modelStream,
        content: { value: 1 },
        shouldIndex: true,
      })

      await waitForEventState(flightClient, documentStream.commit)
      const streamCid = documentStream.baseID.cid.toString()

      // Wait for initial anchor
      console.log('Waiting for initial anchor...')
      const initialResult = await waitForAnchoredEvent(flightClient, streamCid, ANCHOR_TIMEOUT_MS)
      expect(initialResult.anchored).toBe(true)
      expect(initialResult.chainId).toBe(EXPECTED_CHAIN_ID)

      const initialCount = await countAnchoredEvents(flightClient, streamCid)
      console.log(`Initial anchored events count: ${initialCount}`)

      // Update the document
      console.log('Updating document...')
      const updatedState = await modelInstanceClient.updateDocument({
        streamID: documentStream.baseID.toString(),
        newContent: { value: 2 },
        shouldIndex: true,
      })

      await waitForEventState(flightClient, updatedState.commitID.commit)
      console.log('Document updated, waiting for anchor...')

      // Wait for the update to be anchored (should result in more anchored events)
      const startTime = Date.now()
      let newCount = initialCount
      while (Date.now() - startTime < ANCHOR_TIMEOUT_MS) {
        newCount = await countAnchoredEvents(flightClient, streamCid)
        if (newCount > initialCount) {
          break
        }
        await delayMs(POLL_INTERVAL_MS)
      }

      expect(newCount).toBeGreaterThan(initialCount)
      console.log(`Update anchored. Anchored events count: ${newCount}`)
    },
    ANCHOR_TIMEOUT_MS * 2 + 60000,
  )

  test(
    'multiple documents are anchored',
    async () => {
      // Create multiple documents
      console.log('Creating 3 documents...')
      const docs = await Promise.all([
        modelInstanceClient.createInstance({
          model: modelStream,
          content: { value: 10 },
          shouldIndex: true,
        }),
        modelInstanceClient.createInstance({
          model: modelStream,
          content: { value: 20 },
          shouldIndex: true,
        }),
        modelInstanceClient.createInstance({
          model: modelStream,
          content: { value: 30 },
          shouldIndex: true,
        }),
      ])

      // Wait for all init events to be processed
      await Promise.all(docs.map((doc) => waitForEventState(flightClient, doc.commit)))
      console.log('All documents created')

      // Wait for all to be anchored
      const streamCids = docs.map((doc) => doc.baseID.cid.toString())

      console.log('Waiting for all documents to be anchored...')
      const anchorResults = await Promise.all(
        streamCids.map((cid) => waitForAnchoredEvent(flightClient, cid, ANCHOR_TIMEOUT_MS)),
      )

      // Verify all were anchored with correct chain ID
      for (let i = 0; i < anchorResults.length; i++) {
        expect(anchorResults[i].anchored).toBe(true)
        expect(anchorResults[i].chainId).toBe(EXPECTED_CHAIN_ID)
        console.log(
          `Document ${i + 1} anchored: ${docs[i].baseID.toString()} with chain_id: ${anchorResults[i].chainId}`,
        )
      }
    },
    ANCHOR_TIMEOUT_MS + 60000,
  )
})
