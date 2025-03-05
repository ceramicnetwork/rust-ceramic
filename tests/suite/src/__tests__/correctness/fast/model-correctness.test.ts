import { describe, test, beforeAll, expect } from '@jest/globals'
import { loadDocumentOrTimeout, newCeramic } from '../../../utils/ceramicHelpers.js'
import { createDid } from '../../../utils/didHelper.js'
import { StreamID } from '@ceramicnetwork/streamid'
import { Model } from '@ceramicnetwork/stream-model'
import { ModelInstanceDocument } from '@ceramicnetwork/stream-model-instance'
import { LIST_MODEL_DEFINITION } from '../../../models/modelConstants'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { CommonTestUtils as TestUtils } from '@ceramicnetwork/common-test-utils'
import { indexModelOnNode } from '../../../utils/composeDbHelpers.js'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')
const SYNC_TIMEOUT_MS = 30 * 1000

describe('Model Integration Test', () => {
  let ceramicNode1: CeramicClient
  let ceramicNode2: CeramicClient
  let modelId: StreamID
  beforeAll(async () => {
    const did1 = await createDid(adminSeeds[0])
    if (!adminSeeds[1])
      throw new Error(
        'adminSeeds expects minimum 2 dids one for each url, adminSeeds[1] is not set',
      )
    const did2 = await createDid(adminSeeds[1])
    ceramicNode1 = await newCeramic(ComposeDbUrls[0], did1)
    ceramicNode2 = await newCeramic(ComposeDbUrls[1], did2)
    const model = await Model.create(ceramicNode1, LIST_MODEL_DEFINITION)
    modelId = model.id

    await indexModelOnNode(ceramicNode1, model.id)
    await indexModelOnNode(ceramicNode2, model.id)
  })

  test('ModelInstanceDocuments sync between nodes', async () => {
    const modelInstanceDocumentMetadata = { model: modelId }
    const document1 = await ModelInstanceDocument.create(
      ceramicNode1,
      { step: 1 },
      modelInstanceDocumentMetadata,
      { anchor: false },
    )
    const document2 = (await loadDocumentOrTimeout(
      ceramicNode2,
      document1.id,
      SYNC_TIMEOUT_MS,
    )) as ModelInstanceDocument
    expect(document2.id).toEqual(document1.id)
    expect(document1.content).toEqual(document2.content)

    // Now update on the second node and ensure update syncs back to the first node.
    await document2.replace({ step: 2 }, null, { anchor: false })
    await TestUtils.waitForConditionOrTimeout(async () => {
      await document1.sync()
      return document1.content?.step == 2
    })
    expect(document1.content).toEqual(document2.content)
    expect(document1.state.log.length).toEqual(2)
    expect(document2.state.log.length).toEqual(2)
  })
})
