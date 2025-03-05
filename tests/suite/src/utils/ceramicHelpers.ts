import { CeramicClient } from '@ceramicnetwork/http-client'
import { randomBytes } from 'crypto'
import { DID } from 'dids'
import { Ed25519Provider } from 'key-did-provider-ed25519'
import KeyDidResolver from 'key-did-resolver'
import { AnchorStatus, Stream, StreamState, StreamUtils } from '@ceramicnetwork/common'
import { filter, take } from 'rxjs/operators'
import { StreamID } from '@ceramicnetwork/streamid'
import { utilities } from './common.js'

const delayMs = utilities.delayMs

const seed = randomBytes(32)
const provider = new Ed25519Provider(seed)
const resolver = KeyDidResolver.getResolver()
const did = new DID({ provider, resolver })

// 30 minutes for anchors to happen and be noticed (including potential failures and retries)
export const DEFAULT_ANCHOR_TIMEOUT_MS = 60 * 30 * 1000

export const newCeramic = async (apiUrl: string, didOverride?: DID) => {
  const ceramic = new CeramicClient(apiUrl, { syncInterval: 500 })
  const effectiveDID = didOverride || did
  if (!effectiveDID.authenticated) {
    await effectiveDID.authenticate()
  }
  ceramic.did = effectiveDID
  return ceramic
}

function defaultMsgGenerator(stream: Stream) {
  const curTime = new Date().toISOString()
  return `Waiting for stream ${stream.id.toString()} to hit a specific stream state. Current time: ${curTime}. Current stream state: ${JSON.stringify(
    StreamUtils.serializeState(stream.state),
  )}`
}

async function withTimeout(prom: Promise<any>, timeoutMs: number) {
  const startTime = new Date().toISOString()
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      const curTime = new Date().toISOString()
      reject(
        `Timed out after ${timeoutMs} millis. Current time: ${curTime}, start time: ${startTime}`,
      )
    }, timeoutMs)
    prom.then(resolve)
  })
}

/**
 * Loads a document from a ceramic node with a timeout.
 * @param ceramicNode : CeramicClient to load the document from
 * @param documentId : ID of the document to load
 * @param timeoutMs : Timeout in milliseconds
 * @returns The document if found, throws error on timeout
 */
export async function loadDocumentOrTimeout(
  ceramicNode: CeramicClient,
  documentId: StreamID,
  timeoutMs: number,
): Promise<Stream> {
  let now = Date.now()
  let count = 0
  const expirationTime = now + timeoutMs
  let lastError = null
  while (now < expirationTime) {
    try {
      count += 1
      return await ceramicNode.loadStream(documentId)
    } catch (error) {
      lastError = error
      if (count % 10 === 0) {
        console.log(`Error loading document : ${documentId} retrying`, error)
      }
      await delayMs(100)
      now = Date.now()
    }
  }
  throw Error(
    `Timeout waiting for document ${documentId}. Last seen error when trying to load it: ${lastError}`,
  )
}

/**
 * Waits for 'timeoutSecs' for the given 'condition' to evaluate to try when applied to the current
 * stream state for 'stream'.
 * @param stream
 * @param condition
 * @param timeoutMs
 * @param msgGenerator - Function that takes a stream and returns a string to log every time
 *   a new state is found that *doesn't* satisfy 'condition'
 */
export async function waitForCondition(
  stream: Stream,
  condition: (stream: StreamState) => boolean,
  timeoutMs: number,
  msgGenerator?: (stream: Stream) => string,
): Promise<void> {
  const waiter = stream
    .pipe(
      filter((state: StreamState) => {
        if (condition(state)) {
          return true
        }
        const msg = msgGenerator ? msgGenerator(stream) : defaultMsgGenerator(stream)
        console.debug(msg)
        return false
      }),
      take(1),
    )
    .toPromise()

  if (!condition(stream.state)) {
    // Only wait if condition isn't already true
    await withTimeout(waiter, timeoutMs)
  }

  const curTime = new Date().toISOString()
  console.debug(
    `Stream ${stream.id.toString()} successfully reached desired state at ${curTime}. Current stream state: ${JSON.stringify(
      StreamUtils.serializeState(stream.state),
    )}`,
  )
}

export async function waitForAnchor(
  stream: any,
  timeoutMs: number = DEFAULT_ANCHOR_TIMEOUT_MS,
): Promise<void> {
  const msgGenerator = function(stream: Stream) {
    const curTime = new Date().toISOString()
    return `Waiting for stream ${stream.id.toString()} to be anchored. Current time: ${curTime}. Current stream state: ${JSON.stringify(
      StreamUtils.serializeState(stream.state),
    )}`
  }
  await waitForCondition(
    stream,
    function(state) {
      return state.anchorStatus == AnchorStatus.ANCHORED
    },
    timeoutMs,
    msgGenerator,
  )
}
