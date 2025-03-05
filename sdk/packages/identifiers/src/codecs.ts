import { type Context, Type, refinement, string } from 'codeco'

import { StreamID } from './stream-id.js'

/**
 * Verify if `input` is a StreamID string.
 * @internal
 */
export function isStreamIDString(input: string): input is string {
  try {
    StreamID.fromString(input)
    return true
  } catch (err) {
    return false
  }
}

/**
 * codeco codec for StreamID string.
 */
export const streamIDString = refinement(
  string,
  isStreamIDString,
  'StreamID-string',
)

/**
 * codeco codec for StreamID encoded as string.
 */
export const streamIDAsString = new Type<StreamID, string, string>(
  'StreamID-as-string',
  (input: unknown): input is StreamID => StreamID.isInstance(input),
  (input: string, context: Context) => {
    try {
      return context.success(StreamID.fromString(input))
    } catch {
      return context.failure()
    }
  },
  (streamId) => {
    return streamId.toString()
  },
)

/**
 * codeco codec for StreamID encoded as Uint8Array bytes.
 */
export const streamIDAsBytes = new Type<
  StreamID,
  Uint8Array,
  StreamID | Uint8Array
>(
  'StreamID-as-bytes',
  (input: unknown): input is StreamID => StreamID.isInstance(input),
  (input: StreamID | Uint8Array, context: Context) => {
    try {
      return context.success(
        StreamID.isInstance(input) ? input : StreamID.fromBytes(input),
      )
    } catch {
      return context.failure()
    }
  },
  (streamId) => streamId.bytes,
)
