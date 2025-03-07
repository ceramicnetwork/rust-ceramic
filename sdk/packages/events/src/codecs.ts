import { streamIDAsBytes } from '@ceramic-sdk/identifiers'
import { JWSSignature, cid, didString, uint8array } from '@didtools/codecs'
import {
  type TypeOf,
  array,
  boolean,
  decode,
  optional,
  sparse,
  strict,
  string,
  tuple,
  unknown,
} from 'codeco'
import 'multiformats' // Import needed for TS reference
import 'ts-essentials' // Import needed for TS reference

/** IPLD DAG-encoded JWS */
export const DagJWS = sparse({
  payload: string,
  signatures: array(JWSSignature),
  link: optional(cid),
})
export type DagJWS = TypeOf<typeof DagJWS>

/** Header structure of Init events */
export const InitEventHeader = sparse(
  {
    controllers: tuple([didString]),
    model: streamIDAsBytes,
    sep: string,
    unique: optional(uint8array),
    context: optional(streamIDAsBytes),
    shouldIndex: optional(boolean),
  },
  'InitEventHeader',
)
export type InitEventHeader = TypeOf<typeof InitEventHeader>

/** Payload structure of Init events */
export const InitEventPayload = sparse(
  {
    data: unknown,
    header: InitEventHeader,
  },
  'InitEventPayload',
)
export type InitEventPayload<T = unknown> = {
  data: T
  header: InitEventHeader
}

/** Signed event structure - equivalent to DagJWSResult in `dids` package */
export const SignedEvent = sparse(
  {
    jws: DagJWS,
    linkedBlock: uint8array,
    cacaoBlock: optional(uint8array),
  },
  'SignedEvent',
)
export type SignedEvent = TypeOf<typeof SignedEvent>

/** Decode the provided `input` as a SignedEvent. Throws if decoding fails. */
export function decodeSignedEvent(input: unknown): SignedEvent {
  return decode(SignedEvent, input)
}

/**
 * @ignore
 */
export function assertSignedEvent(
  input: unknown,
): asserts input is SignedEvent {
  decodeSignedEvent(input)
}

/** Time event structure */
export const TimeEvent = strict(
  {
    id: cid,
    prev: cid,
    proof: cid,
    path: string,
  },
  'TimeEvent',
)
export type TimeEvent = TypeOf<typeof TimeEvent>

/** Decode the provided `input` as a TimeEvent. Throws if decoding fails. */
export function decodeTimeEvent(input: unknown): TimeEvent {
  return decode(TimeEvent, input)
}

/**
 * @ignore
 */
export function assertTimeEvent(input: unknown): asserts input is TimeEvent {
  decodeTimeEvent(input)
}
