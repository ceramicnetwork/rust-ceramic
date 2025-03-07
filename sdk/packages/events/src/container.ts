import { type Decoder, decode } from 'codeco'
import type { DID, VerifyJWSResult } from 'dids'
import type { CID } from 'multiformats/cid'

import { SignedEvent } from './codecs.js'
import { getSignedEventPayload } from './signing.js'

/**
 * A container for a signed Ceramic event.
 *
 * @typeParam Payload - The type of the event's payload.
 *
 * @property signed - Indicates that the event is signed (`true`).
 * @property cid - The CID of the linked block for the event.
 * @property payload - The decoded payload of the event.
 * @property verified - The result of verifying the event's JWS.
 * @property cacaoBlock - (Optional) A block containing a Cacao (Capability Object).
 */
export type SignedEventContainer<Payload> = {
  signed: true
  cid: CID
  payload: Payload
  verified: VerifyJWSResult
  cacaoBlock?: Uint8Array
}

/**
 * A container for an unsigned Ceramic event.
 *
 * @typeParam Payload - The type of the event's payload.
 *
 * @property signed - Indicates that the event is unsigned (`false`).
 * @property payload - The decoded payload of the event.
 */
export type UnsignedEventContainer<Payload> = {
  signed: false
  payload: Payload
}

/**
 * A container for a Ceramic event, which can be either signed or unsigned.
 *
 * @typeParam Payload - The type of the event's payload.
 */
export type EventContainer<Payload> =
  | SignedEventContainer<Payload>
  | UnsignedEventContainer<Payload>

/**
 * Decodes an unsigned Ceramic event into a container.
 *
 * @param codec - The codec used to decode the event's payload.
 * @param event - The unsigned Ceramic event to decode.
 * @returns An `UnsignedEventContainer` containing the decoded payload.
 *
 * @remarks
 * - This function assumes that the event is unsigned and decodes it accordingly.
 * - Use `eventToContainer` if the event type (signed or unsigned) is unknown.
 */
export function unsignedEventToContainer<Payload>(
  codec: Decoder<unknown, Payload>,
  event: unknown,
): UnsignedEventContainer<Payload> {
  return { signed: false, payload: decode(codec, event) }
}

/**
 * Decodes a signed Ceramic event into a container.
 *
 * @param did - The DID used to verify the event's JWS.
 * @param codec - The codec used to decode the event's payload.
 * @param event - The signed Ceramic event to decode.
 * @returns A promise that resolves to a `SignedEventContainer` containing the decoded payload and metadata.
 *
 * @throws Will throw an error if the linked block CID is missing or if verification fails.
 *
 * @remarks
 * - This function verifies the event's JWS and decodes its payload simultaneously.
 * - It also includes additional metadata such as the verification result and `cacaoBlock` if present.
 */
export async function signedEventToContainer<Payload>(
  did: DID,
  codec: Decoder<unknown, Payload>,
  event: SignedEvent,
): Promise<SignedEventContainer<Payload>> {
  const cid = event.jws.link
  if (cid == null) {
    throw new Error('Missing linked block CID')
  }
  const [verified, payload] = await Promise.all([
    did.verifyJWS(event.jws), // Verify the JWS signature.
    getSignedEventPayload(codec, event), // Decode the payload.
  ])
  return { signed: true, cid, verified, payload, cacaoBlock: event.cacaoBlock }
}

/**
 * Decodes a Ceramic event (signed or unsigned) into a container.
 *
 * @param did - The DID used to verify the event's JWS if it is signed.
 * @param codec - The codec used to decode the event's payload.
 * @param event - The Ceramic event to decode (can be signed or unsigned).
 * @returns A promise that resolves to an `EventContainer` containing the decoded payload and metadata.
 *
 * @remarks
 * - This function determines the type of the event (signed or unsigned) and processes it accordingly.
 * - For signed events, it verifies the JWS and decodes the payload.
 * - For unsigned events, it simply decodes the payload.
 */
export async function eventToContainer<Payload>(
  did: DID,
  codec: Decoder<unknown, Payload>,
  event: unknown,
): Promise<EventContainer<Payload>> {
  return SignedEvent.is(event)
    ? await signedEventToContainer(did, codec, event)
    : unsignedEventToContainer(codec, event)
}
