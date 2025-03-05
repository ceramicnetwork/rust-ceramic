import { SignedEvent } from '@ceramic-sdk/events'
import { CeramicClient } from '@ceramic-sdk/http-client'
import { CommitID, type StreamID } from '@ceramic-sdk/identifiers'
import { createInitEvent as createModel } from '@ceramic-sdk/model-client'
import {
  createInitEvent as createDocument,
  createDataEvent as updateDocument,
} from '@ceramic-sdk/model-instance-client'
import { getStreamID } from '@ceramic-sdk/model-instance-protocol'
import {
  type ModelDefinition,
  getModelStreamID,
} from '@ceramic-sdk/model-protocol'
import { getAuthenticatedDID } from '@didtools/key-did'

const DEMO_MODEL: ModelDefinition = {
  version: '2.0',
  name: 'ExplorerDemoModel',
  description: 'Demo model for the Ceramic Explorer UI',
  accountRelation: { type: 'list' },
  interface: false,
  implements: [],
  schema: {
    type: 'object',
    properties: {
      text: { type: 'string', maxLength: 20 },
    },
    additionalProperties: false,
  },
}

const client = new CeramicClient({ url: 'http://localhost:5101' })
const did = await getAuthenticatedDID(new Uint8Array(32))

const modelEvent = await createModel(did, DEMO_MODEL)
const modelCID = await client.postEventType(SignedEvent, modelEvent)
const model = getModelStreamID(modelCID)
console.log(`Created model ${model} (CID: ${modelCID})`)

async function createAndPostDocument(
  content: Record<string, unknown>,
): Promise<StreamID> {
  const event = await createDocument({ controller: did, content, model })
  const cid = await client.postEventType(SignedEvent, event)
  return getStreamID(cid)
}

const docIDs = await Promise.all([
  createAndPostDocument({ text: 'Hello' }),
  createAndPostDocument({ text: 'World' }),
])
console.log(`Created documents with stream IDs: ${docIDs.join(', ')}`)

const updateEvent = await updateDocument({
  controller: did,
  currentID: CommitID.fromStream(docIDs[0]),
  content: { text: 'Hello World' },
})
await client.postEventType(SignedEvent, updateEvent)
console.log(`Updated document with stream ID: ${docIDs[0]}`)
