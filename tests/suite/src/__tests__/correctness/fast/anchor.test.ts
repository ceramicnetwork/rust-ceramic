import { AnchorStatus, StreamUtils } from '@ceramicnetwork/common'
import { afterAll, beforeAll, describe, expect, test } from '@jest/globals'
import { DateTime } from 'luxon'

import { newCeramic } from '../../../utils/ceramicHelpers.js'
import * as helpers from '../../../utils/dynamoDbHelpers.js'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')

describe('anchor', () => {
  beforeAll(async () => await helpers.createTestTable())
  afterAll(async () => await helpers.cleanup())

  test('test anchors', async () => {
    const anchorReqs = await helpers.fetchUnanchoredStreamReqs()
    console.log(`Identified ${anchorReqs.length} streams pending anchor status check`)

    for (const req of anchorReqs) {
      const ceramic = await newCeramic(ComposeDbUrls[0])
      const doc = await ceramic.loadStream(<string>req.StreamId.S)
      console.log(`${doc.id}: anchor status = ${AnchorStatus[doc.state.anchorStatus]}`)
      const now = DateTime.utc()
      const createdAt = DateTime.fromSeconds(parseInt(<string>req.Creation.N))
      const deltaMinutes = now.diff(createdAt).as('minutes')
      const configMinutes = helpers.AnchorInterval.as('minutes')
      // Cleanup the DB entry and conclude the test if the anchor succeeded or timed out (based on the configured
      // interval), otherwise re-check on the next iteration.
      //
      // Don't explicitly check for failures until a timeout because failed requests can be retried and successful
      // on subsequent attempts within the configured interval.
      if (doc.state.anchorStatus == AnchorStatus.ANCHORED || deltaMinutes >= configMinutes) {
        try {
          if (doc.state.anchorStatus != AnchorStatus.ANCHORED) {
            // If the stream wasn't already anchored, make sure we haven't been waiting too long. This check
            // will also catch anchor failures (i.e. requests for which anchoring was attempted but failed
            // even after retries).
            expect(deltaMinutes).toBeLessThan(configMinutes)
          }

          await helpers.markStreamReqAsAnchored(doc.id)
        } catch (err) {
          console.error(
            `Test failed. StreamID: ${doc.id.toString()}, state:\n${JSON.stringify(
              StreamUtils.serializeState(doc.state),
              null,
              2,
            )}`,
          )

          // If the anchoring failed, we don't want to leave this stream in the database,
          // as it will cause all future test executions to fail as well.
          await helpers.deleteStreamReq(doc.id)

          throw err
        }
      }
    }
  })
})
