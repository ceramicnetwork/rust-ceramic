import { StreamID } from '@ceramic-sdk/identifiers'
import type { CID } from 'multiformats/cid'

import { DocumentInitEventHeader } from './codecs.js'
import { STREAM_TYPE_ID } from './constants.js'

/**
 * Get the StreamID of a deterministic ModelInstanceDocument based on its init header
 */
export function getDeterministicStreamID(
  header: DocumentInitEventHeader,
): StreamID {
  // Generate deterministic stream ID from specific header keys
  const initHeader = DocumentInitEventHeader.encode({
    controllers: header.controllers,
    model: header.model,
    sep: header.sep,
    unique: header.unique,
  })
  return StreamID.fromPayload(STREAM_TYPE_ID, {
    data: null,
    header: initHeader,
  })
}

/**
 * Get the StreamID of a ModelInstanceDocument based on its init CID
 */
export function getStreamID(cid: CID): StreamID {
  return new StreamID(STREAM_TYPE_ID, cid)
}
