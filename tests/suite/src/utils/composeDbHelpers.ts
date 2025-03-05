import { ComposeClient } from '@composedb/client'
import { utilities } from './common.js'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { StreamID } from '@ceramicnetwork/streamid'
import { CommonTestUtils as TestUtils } from '@ceramicnetwork/common-test-utils'

const delay = utilities.delay
const delayMs = utilities.delayMs

/**
 * Waits for a specific document to be available by repeatedly querying the ComposeDB until the document is found or a timeout occurs.
 *
 * This function continuously executes a provided GraphQL query using the ComposeClient until a document with an `id` is found in the response.
 * If the document is found within the specified timeout period, the function returns the document's response object.
 * If the document is not found within the timeout period, the function throws a timeout error.
 *
 * @param {ComposeClient} composeClient - The ComposeClient instance used to execute the query.
 * @param {string} query - The GraphQL query string to be executed.
 * @param {any} variables - The variables to be passed with the query.
 * @param {number} timeoutMs - The maximum amount of time (in milliseconds) to wait for the document before timing out.
 * @returns {Promise<any>} A promise that resolves with the response object of the document if found within the timeout period.
 * @throws {Error} Throws an error if the document is not found within the specified timeout period.
 */

export async function waitForDocument(
  composeClient: ComposeClient,
  query: string,
  variables: any,
  timeoutMs: number,
) {
  const startTime = Date.now()
  while (true) {
    const response = await composeClient.executeQuery(query, variables)
    const responseObj = JSON.parse(JSON.stringify(response))
    if (responseObj?.data?.node?.id) {
      return responseObj
    }
    if (Date.now() - startTime > timeoutMs) {
      throw new Error('Timeout waiting for document')
    }
    await delay(1)
  }
}

/**
 * Checks if a model is indexed on a Ceramic node.
 * @param ceramicNode : Ceramic client to check indexing on
 * @param modelId : ID of the model to check indexing for
 * @returns True if the model is indexed on the node, false otherwise
 */
async function isModelIndexed(ceramicNode: CeramicClient, modelId: StreamID): Promise<boolean> {
  const indexedModels = await ceramicNode.admin.getIndexedModelData()
  for (const m of indexedModels) {
    const streamId = m.streamID
    if (streamId.toString() === modelId.toString()) {
      return true
    }
  }
  return false
}

/**
 * Waits for indexing to complete on both nodes or a timeout.
 * @param ceramicNode : Ceramic client to check indexing on
 * @param modelId : ID of the model to check indexing for
 * @param timeoutMs : Timeout in milliseconds
 * @returns True if indexing is complete, throws error on timeout
 */
export async function waitForIndexingOrTimeout(
  ceramicNode: CeramicClient,
  modelId: StreamID,
  timeoutMs: number,
): Promise<boolean> {
  const startTime = Date.now()
  const expirationTime = startTime + timeoutMs

  while (Date.now() < expirationTime) {
    const isIndexedOnNode1 = await isModelIndexed(ceramicNode, modelId)

    if (isIndexedOnNode1) {
      return true
    }

    await delayMs(100)
    console.log(`Waiting for indexing model: ${modelId} on both ceramic nodes`)
  }

  throw new Error(`Timeout waiting for indexing model: ${modelId}`)
}

export async function indexModelOnNode(
  node: CeramicClient,
  modelId: StreamID,
  timeoutMs = 1000 * 10,
): Promise<void> {
  await TestUtils.waitForConditionOrTimeout(async () =>
    node
      .loadStream(modelId)
      .then((_) => true)
      .catch((_) => false),
  )
  await node.admin.startIndexingModels([modelId])
  await waitForIndexingOrTimeout(node, modelId, timeoutMs)
}
