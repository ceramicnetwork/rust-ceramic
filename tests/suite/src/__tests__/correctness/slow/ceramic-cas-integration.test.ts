import { AnchorStatus } from '@ceramicnetwork/common'
import { describe, test, beforeAll, expect, jest } from '@jest/globals'
import { newCeramic, waitForAnchor } from '../../../utils/ceramicHelpers.js'
import { createDid } from '../../../utils/didHelper.js'
import { Model } from '@ceramicnetwork/stream-model'
import { LIST_MODEL_DEFINITION } from '../../../models/modelConstants.js'
import { indexModelOnNode } from '../../../utils/composeDbHelpers.js'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { ModelInstanceDocument } from '@ceramicnetwork/stream-model-instance'
import { StreamID } from '@ceramicnetwork/streamid'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')

// Skipped https://linear.app/3boxlabs/issue/WS1-1460/unskip-ceramic-cas-basic-integration
describe('Ceramic<->CAS basic integration', () => {
  jest.setTimeout(1000 * 60 * 30) // 30 minutes (default max wait time for an anchor is 30 minutes)

  let ceramic: CeramicClient
  let modelId: StreamID

  beforeAll(async () => {
    const did = await createDid(adminSeeds[0])
    ceramic = await newCeramic(ComposeDbUrls[0], did)
    const model = await Model.create(ceramic, LIST_MODEL_DEFINITION)
    modelId = model.id
    await indexModelOnNode(ceramic, model.id)
  })

  test('basic crud is anchored properly, single update per anchor batch', async () => {
    // Test document creation
    console.log('Creating document')
    const initialContent = { step: 0 }
    const metadata = { controllers: [ceramic.did!.id], model: modelId }
    const doc = await ModelInstanceDocument.create(ceramic, initialContent, metadata)
    expect(doc.content).toEqual(initialContent)

    // Test document creation is anchored correctly
    console.log('Waiting for anchor of genesis record')
    await waitForAnchor(doc)
    expect(doc.state.log.length).toEqual(2)

    // Test document update
    console.log('Updating document')
    const newContent = { step: 1 }
    await doc.replace(newContent)
    expect(doc.content).toEqual(newContent)

    // Test document update is anchored correctly
    console.log('Waiting for anchor of update')
    await waitForAnchor(doc)
    expect(doc.content).toEqual(newContent)
    expect(doc.state.log.length).toEqual(4)
  })

  test('multiple documents are anchored properly', async () => {
    const content0 = { step: 0 }
    const content1 = { step: 1 }
    const content2 = { step: 2 }
    const content3 = { step: 3 }
    const content4 = { step: 4 }
    const metadata = { controllers: [ceramic.did!.id], model: modelId }

    // Create some documents
    console.log('Creating documents')
    const doc1 = await ModelInstanceDocument.create(ceramic, content0, metadata)
    const doc2 = await ModelInstanceDocument.create(ceramic, content0, metadata)
    const doc3 = await ModelInstanceDocument.create(ceramic, content0, metadata)
    const doc4 = await ModelInstanceDocument.create(ceramic, content0, metadata)
    expect(doc1.content).toEqual(content0)
    expect(doc2.content).toEqual(content0)
    expect(doc3.content).toEqual(content0)
    expect(doc4.content).toEqual(content0)

    // Test document creation is anchored correctly
    console.log('Waiting for anchor of genesis records')
    await waitForAnchor(doc1)
    await waitForAnchor(doc2)
    await waitForAnchor(doc3)
    await waitForAnchor(doc4)

    expect(doc1.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc1.state.log.length).toEqual(2)
    expect(doc2.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc2.state.log.length).toEqual(2)
    expect(doc3.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc3.state.log.length).toEqual(2)
    expect(doc4.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc4.state.log.length).toEqual(2)

    // Test document updates
    // The anchor option is no longer honored when ceramic-one does the anchroing.
    console.log('Updating documents')
    await doc1.replace(content1)
    await doc2.replace(content2)
    await doc3.replace(content3)
    await doc4.replace(content4)

    // Test document updates are anchored correctly
    console.log('Waiting for anchor of updates')

    await waitForAnchor(doc1)
    await waitForAnchor(doc2)
    await waitForAnchor(doc3)
    await waitForAnchor(doc4)


    expect(doc1.content).toEqual(content1)
    expect(doc2.content).toEqual(content2)
    expect(doc3.content).toEqual(content3)
    expect(doc4.content).toEqual(content4)


    expect(doc1.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc2.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc3.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc4.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)

    expect(doc1.state.log.length).toEqual(4)
    expect(doc2.state.log.length).toEqual(4)
    expect(doc3.state.log.length).toEqual(4)
    expect(doc4.state.log.length).toEqual(4)
  })
})
