import { StreamReaderWriter, SyncOptions } from '@ceramicnetwork/common'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { afterAll, beforeAll, expect, test, describe } from '@jest/globals'

import * as helpers from '../../../utils/dynamoDbHelpers.js'
import { loadDocumentOrTimeout, newCeramic, waitForCondition } from '../../../utils/ceramicHelpers.js'
import { createDid } from '../../../utils/didHelper.js'
import { LIST_MODEL_DEFINITION } from '../../../models/modelConstants.js'
import { Model } from '@ceramicnetwork/stream-model'
import { indexModelOnNode } from '../../../utils/composeDbHelpers.js'
import { StreamID } from '@ceramicnetwork/streamid'
import { ModelInstanceDocument } from '@ceramicnetwork/stream-model-instance'

// Environment variables
const composeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')
const SYNC_TIMEOUT_MS = 30 * 1000 // 30 seconds

///////////////////////////////////////////////////////////////////////////////
/// Create/Update Tests
///////////////////////////////////////////////////////////////////////////////

let modelId: StreamID

describe('update', () => {
  beforeAll(async () => {
    await helpers.createTestTable()

    if (adminSeeds.length < composeDbUrls.length) {
      throw new Error(
        `Must provide an admin DID seed for each node. Number of nodes: ${composeDbUrls.length}, number of seeds: ${adminSeeds.length}`,
      )
    }

    const did0 = await createDid(adminSeeds[0])
    const ceramicNode0 = await newCeramic(composeDbUrls[0], did0)
    const model = await Model.create(ceramicNode0, LIST_MODEL_DEFINITION)
    modelId = model.id
    await indexModelOnNode(ceramicNode0, model.id)

    for (let i = 1; i < composeDbUrls.length; i++) {
      const did = await createDid(adminSeeds[i])
      const node = await newCeramic(composeDbUrls[i], did)
      await indexModelOnNode(node, model.id)
    }
  })
  afterAll(async () => await helpers.cleanup())

  // Run tests with each node being the node where a stream is created
  generateUrlCombinations(composeDbUrls).forEach(testUpdate)
})

function generateUrlCombinations(urls: string[]): string[][] {
  return urls.map((_, index) => [...urls.slice(index), ...urls.slice(0, index)])
}

function testUpdate(composeDbUrls: string[]) {
  console.log(`test update with urls: ${composeDbUrls}`)
  const firstNodeUrl = composeDbUrls[0]
  const content = { step: 0 }
  let firstCeramic: CeramicClient
  let firstDocument: ModelInstanceDocument

  // Create and update on first node
  test(`create stream on ${firstNodeUrl}`, async () => {
    firstCeramic = await newCeramic(firstNodeUrl)
    const metadata = { controllers: [firstCeramic.did!.id], model: modelId }
    firstDocument = await ModelInstanceDocument.create(
      firstCeramic as StreamReaderWriter,
      content,
      metadata,
      {
        anchor: false,
      },
    )
    console.log(
      `Created stream on ${firstNodeUrl}: ${firstDocument.id.toString()} with step ${content.step}`,
    )
  })

  test(`update stream on ${firstNodeUrl}`, async () => {
    content.step++
    await firstDocument.replace(content, undefined, { anchor: false })
    console.log(
      `Updated stream on ${firstNodeUrl}: ${firstDocument.id.toString()} with step ${content.step}`,
    )
  })

  // Test load, update, and sync on subsequent node(s)
  // Skip first url because it was already handled in the previous tests
  for (let idx = 1; idx < composeDbUrls.length; idx++) {
    const apiUrl = composeDbUrls[idx]
    let doc: ModelInstanceDocument
    test(`load stream on ${apiUrl}`, async () => {
      const ceramic = await newCeramic(apiUrl)
      console.log(
        `Loading stream ${firstDocument.id.toString()} on ${apiUrl} with step ${content.step}`,
      )
      doc = (await loadDocumentOrTimeout(
        ceramic,
        firstDocument.id,
        SYNC_TIMEOUT_MS,
      )) as ModelInstanceDocument
      await waitForCondition(doc, (state) => state.content.step == content.step, SYNC_TIMEOUT_MS)
      await doc.sync({ sync: SyncOptions.NEVER_SYNC })
      expect(doc.content).toEqual(content)
      console.log(
        `Loaded stream on ${apiUrl}: ${firstDocument.id.toString()} successfully with step ${
          content.step
        }`,
      )
    })
    test(`sync stream on ${apiUrl}`, async () => {
      const isFinalWriter = idx == composeDbUrls.length - 1
      // Update the content as we iterate through the list of node URLs so that each step includes some change
      // from the previous step.
      content.step++
      // Update on first node and wait for update to propagate to other nodes via pubsub
      // Only anchor on the final write to avoid writes conflicting with anchors.
      console.log(
        `Updating stream ${firstDocument.id.toString()} on ${firstNodeUrl} so we can sync it on ${apiUrl} with step ${
          content.step
        }`,
      )
      await firstDocument.replace(content, undefined, { anchor: isFinalWriter })
      await waitForCondition(doc, (state) => state.content.step == content.step, SYNC_TIMEOUT_MS)
      await doc.sync({ sync: SyncOptions.NEVER_SYNC })
      expect(doc.content).toEqual(firstDocument.content)
      console.log(
        `Synced stream on ${apiUrl}: ${firstDocument.id.toString()} successfully with step ${
          content.step
        }`,
      )

      if (isFinalWriter) {
        // Store the anchor request in the DB
        await helpers.storeStreamReq(firstDocument.id)
      }
    })
  }
}
