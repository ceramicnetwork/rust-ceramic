import { StreamID } from '@ceramicnetwork/streamid'
import { afterAll, beforeAll, describe, expect, test } from '@jest/globals'

import * as helpers from '../../../utils/dynamoDbHelpers.js'

const streamID0 = StreamID.fromString(
  'kjzl6cwe1jw149mj7gw90a40vf9qoipv1gthagyzxzjrg8pp9htbdbvu2er1wan',
)
const streamID1 = StreamID.fromString(
  'kjzl6cwe1jw149zfreoiia0ayfbwxnc3qizfyouw1c6mp9e3ah3smmhqfrppm0z',
)

/**
 * Tests the behaviors of the 'helpers' functions for doing reads and writes to
 * DynamoDB.  Not meant to be run in production, just used for local testing
 * during development.
 */
describe('tests helpers functions for interacting with DynamoDB', () => {
  beforeAll(async () => await helpers.createTestTable())
  afterAll(async () => {
    await helpers.deleteStreamReq(streamID0)
    await helpers.deleteStreamReq(streamID1)

    await helpers.cleanup()
  })

  test.skip('query pagination', async () => {
    await helpers.storeStreamReq(streamID0)
    await helpers.storeStreamReq(streamID1)

    // Setting page size to 1 forces the query to process multiple batches in order to find
    // and return all (2) results.
    const unanchoredReqs = await helpers.fetchUnanchoredStreamReqs(1)
    expect(unanchoredReqs.length).toEqual(2)
  })

  test.skip('anchoring flow', async () => {
    await helpers.storeStreamReq(streamID0)
    await helpers.storeStreamReq(streamID1)

    let unanchoredReqs = await helpers.fetchUnanchoredStreamReqs()
    expect(unanchoredReqs.length).toEqual(2)

    await helpers.markStreamReqAsAnchored(streamID0)

    unanchoredReqs = await helpers.fetchUnanchoredStreamReqs()
    expect(unanchoredReqs.length).toEqual(1)
  })

  test.skip('expiration flow', async () => {
    await helpers.storeStreamReq(streamID0)
    await helpers.storeStreamReq(streamID1)

    let expiredReqs = await helpers.fetchExpiredStreamReqs()
    expect(expiredReqs.length).toEqual(0)

    await helpers.makeStreamReqAppearExpired_FOR_TESTING_ONLY(streamID0)

    expiredReqs = await helpers.fetchExpiredStreamReqs()
    expect(expiredReqs.length).toEqual(1)
  })
})
