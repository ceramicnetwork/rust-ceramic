import { CeramicClient } from '@ceramicnetwork/http-client'
import { utilities } from '../../utils/common.js'
import { test, describe, expect } from '@jest/globals'
import { Model } from '@ceramicnetwork/stream-model'
import { AccountId } from 'caip'
import { HDNodeWallet, Wallet } from 'ethers'
import { EthereumNodeAuth } from '@didtools/pkh-ethereum'
import { DIDSession } from 'did-session'
import { StreamID } from '@ceramicnetwork/streamid'
import { ModelInstanceDocument } from '@ceramicnetwork/stream-model-instance'
import * as u8s from 'uint8arrays'

import { newCeramic, waitForAnchor } from '../../utils/ceramicHelpers.js'
import { createDid } from '../../utils/didHelper.js'
import { SINGLE_MODEL_DEFINITION, LIST_MODEL_DEFINITION } from '../../models/modelConstants.js'
import { indexModelOnNode } from '../../utils/composeDbHelpers.js'

const delay = utilities.delay


// Environment variables
const ceramicUrls = String(process.env.CERAMIC_URLS).split(',')
const composeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')

class MockProvider {
  wallet: HDNodeWallet

  constructor(wallet: HDNodeWallet) {
    this.wallet = wallet
  }

  send(
    request: { method: string; params: Array<any> },
    callback: (err: Error | null | undefined, res?: any) => void,
  ): void {
    if (request.method === 'eth_chainId') {
      callback(null, { result: '1' })
    } else if (request.method === 'personal_sign') {
      let message = request.params[0] as string
      if (message.startsWith('0x')) {
        message = u8s.toString(u8s.fromString(message.slice(2), 'base16'), 'utf8')
      }
      callback(null, { result: this.wallet.signMessage(message) })
    } else {
      callback(new Error(`Unsupported method: ${request.method}`))
    }
  }
}

async function getVersion(url: string) {
  try {
    let response = await fetch(url + `/api/v0/id`, { method: 'POST' })
    let info = await response.json()
    return info.AgentVersion
  } catch {
    return undefined
  }
}
async function waitForVersionChange(url: string, prevVersion: string) {
  let version = await getVersion(url)
  while (version === undefined || version === prevVersion) {
    console.log('waiting for version change', prevVersion)
    await delay(5)
    version = await getVersion(url)
  }
  console.log('version changed', version)
}

async function waitForNodeAlive(url: string) {
  while (true) {
    try {
      console.log('waiting for node to be alive')
      let response = await fetch(url + `/api/v0/node/healthcheck`)
      let info = await response.text()
      if (info == 'Alive!') {
        return
      }
      await delay(5)
    } catch {
      //Ignore all other errors
    }
  }
}

async function writeKeySignedDataEvents(url: string, listModelId: StreamID) {
  // did:key
  const did = await createDid()
  let ceramic = await newCeramic(url, did)

  // did:key signed init event
  const content = { step: 400 }
  const metadata = { controllers: [did.id], model: listModelId }
  const doc = await ModelInstanceDocument.create(ceramic, content, metadata, { anchor: true })

  // Ensure the init event is anchored
  await waitForAnchor(doc)

  const loadedDoc = await ModelInstanceDocument.load(ceramic, doc.id)
  expect(loadedDoc.content).toEqual(content)

  // did:key signed data event
  const content2 = { step: 401 }
  await doc.replace(content2, null, { anchor: true })

  // Ensure the data event is anchored
  await waitForAnchor(doc)

  return { doc: loadedDoc as ModelInstanceDocument<Record<string, any>>, content: content2 }
}

async function readKeySignedDataEvents(loadedDoc: ModelInstanceDocument<Record<string, any>>, content: Record<string, any>) {
  await loadedDoc.sync()
  expect(loadedDoc.content).toEqual(content)
}



async function writeCACAOSignedDataEvents(url: string, listModelId: StreamID) {
  // did:pkh + cacao
  const wallet = Wallet.createRandom()
  const provider = new MockProvider(wallet)
  const accountId = new AccountId({
    address: wallet.address.toLowerCase(),
    chainId: { namespace: 'eip155', reference: '1' },
  })
  const authMethod = await EthereumNodeAuth.getAuthMethod(provider, accountId, 'test')
  const resources = [`ceramic://*`]
  const session = await DIDSession.authorize(authMethod, {
    resources,
  })
  let ceramic = await newCeramic(url, session.did)

  // cacao signed init event
  const content = { step: 600 }
  const metadata = {
    controllers: [`did:pkh:eip155:1:${wallet.address.toLowerCase()}`],
    model: listModelId,
  }
  const doc = await ModelInstanceDocument.create(ceramic, content, metadata, { anchor: true })

  // Ensure the init event is anchored
  await waitForAnchor(doc)

  const loadedDoc = await ModelInstanceDocument.load(ceramic, doc.id)
  expect(loadedDoc.content).toEqual(content)

  // cacao signed data event
  const content2 = { step: 601 }
  await doc.replace(content2, null, { anchor: true })

  // Ensure the data event is anchored
  await waitForAnchor(doc)

  return { doc: loadedDoc as ModelInstanceDocument<Record<string, any>>, content: content2 }
}
async function readCACAOSignedDataEvents(loadedDoc: ModelInstanceDocument<Record<string, any>>, content: Record<string, any>) {
  await loadedDoc.sync()
  expect(loadedDoc.content).toEqual(content)
}


async function writeUnsignedInitEvents(url: string, singleModelId: StreamID) {
  const did = await createDid()
  let ceramic = await newCeramic(url, did)

  // single/deterministic model instance documents are unsigned
  const metadata = { controllers: [did.id], model: singleModelId, deterministic: true }
  const doc = await ModelInstanceDocument.single(ceramic, metadata, {
    anchor: true,
  })

  // Ensure the init event is anchored
  await waitForAnchor(doc)

  // did:key signed data event
  const content = { step: 700 }
  await doc.replace(content, null, { anchor: true })

  // Ensure the data event is anchored
  await waitForAnchor(doc)

  return {
    ceramic,
    id: doc.id,
    content
  }
}

async function readUnsignedInitEvents(ceramic: CeramicClient, id: StreamID, content: Record<string, any>) {
  const loadedDoc = await ModelInstanceDocument.load(ceramic, id)
  expect(loadedDoc.content).toEqual(content)
}

describe('All Event Types', () => {
  let ceramic: CeramicClient
  let singleModelId: StreamID
  let listModelId: StreamID
  let ceramicVersion: string

  test('migrate', async () => {
    // Setup client and models
    ceramicVersion = await getVersion(ceramicUrls[0])
    const did = await createDid(adminSeeds[0])
    ceramic = await newCeramic(composeDbUrls[0], did)

    const singleModel = await Model.create(ceramic, SINGLE_MODEL_DEFINITION)
    singleModelId = singleModel.id
    await indexModelOnNode(ceramic, singleModelId)

    const listModel = await Model.create(ceramic, LIST_MODEL_DEFINITION)
    listModelId = listModel.id
    await indexModelOnNode(ceramic, listModelId)

    // Write data of all event types
    // Each write is anchored ensuring we have time events with a prev pointing to all types of events
    let [key, cacao, unsigned] = await Promise.all([
      writeKeySignedDataEvents(composeDbUrls[0], listModelId),
      writeCACAOSignedDataEvents(composeDbUrls[0], listModelId),
      writeUnsignedInitEvents(composeDbUrls[0], singleModelId)
    ]);

    // Wait for the version change to indicate a migration has completed
    await waitForVersionChange(ceramicUrls[0], ceramicVersion)
    // Wait for js-ceramic node to be alive before continuing
    await waitForNodeAlive(composeDbUrls[0])

    // Read previously written data
    await readKeySignedDataEvents(key.doc, key.content)
    await readCACAOSignedDataEvents(cacao.doc, cacao.content)
    await readUnsignedInitEvents(unsigned.ceramic, unsigned.id, unsigned.content)
  })
})
