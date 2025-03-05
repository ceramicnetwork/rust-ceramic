// 3rd party dependencies
import { afterAll, beforeAll, describe, test } from '@jest/globals'

// Internal dependencies
import { CeramicClient } from '@ceramicnetwork/http-client'
import { StreamID } from '@ceramicnetwork/streamid'
import { newCeramic } from '../../../utils/ceramicHelpers.js'
import * as helpers from '../../../utils/dynamoDbHelpers.js'

// Environment variables
const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')

/**
 * TODO: Skipping this test for now because it is not required and needs some
 * refactoring. The "pin rm" function is now behind the admin API so the Ceramic
 * client used in this test will need the same DID used by the daemon the request
 * is being made to.
 *
 * This isn't really a test, but a process that gets run by the test harness to clean up old streams
 * that were created by previous runs of the test.  Each time the tests run, streams get created
 * and pinned against the nodes being tested against.  We record those streams in the 'Anchor'
 * table in DynamoDB. Some tests will load streams that were created by previous runs of the test,
 * for instance to ensure that they got anchored some time since the test run that orignally created
 * them.  This 'test' exists solely to look for streams in the database that have been around long
 * enough for all the tests that needed them, and can now be garbage collected.  Garbage collection
 * involves unpinning them from all nodes, and removing the corresponding entry from the database.
 */
describe.skip('garbage collect old streams created by tests', () => {
  let ceramicConnections: CeramicClient[] = []

  beforeAll(async () => {
    await helpers.createTestTable()
    for (const url of ComposeDbUrls) {
      const ceramic = await newCeramic(url)
      ceramicConnections.push(ceramic)
    }
  })

  afterAll(async () => await helpers.cleanup())

  test('garbage collection', async () => {
    const expiredReqs = await helpers.fetchExpiredStreamReqs()
    console.log(`Found ${expiredReqs.length} expired streams in database to garbage collect`)

    for (const req of expiredReqs) {
      const streamId = StreamID.fromString(<string>req.StreamId.S)
      for (const ceramic of ceramicConnections) {
        await ceramic.pin.rm(streamId)
      }

      await helpers.deleteStreamReq(streamId)
    }
  })
})
