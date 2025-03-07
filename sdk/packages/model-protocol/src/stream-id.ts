import { StreamID } from '@ceramic-sdk/identifiers'
import type { CID } from 'multiformats/cid'

import { STREAM_TYPE_ID } from './constants.js'

/**
 * Get a model StreamID from its init event CID
 * @param cid - the CID of the init event
 */
export function getModelStreamID(cid: CID): StreamID {
  return new StreamID(STREAM_TYPE_ID, cid)
}
