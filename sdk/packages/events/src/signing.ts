import { type DIDString, asDIDString } from '@didtools/codecs'
import * as dagCbor from '@ipld/dag-cbor'
import { type Decoder, decode } from 'codeco'
import type { CreateJWSOptions, DID } from 'dids'
import * as Block from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'

import {
  type InitEventHeader,
  InitEventPayload,
  type SignedEvent,
} from './codecs.js'

/**
 * Signs an event payload using the provided DID.
 *
 * @param did - The DID instance used to sign the payload.
 * @param payload - The data to be signed, provided as a key-value map.
 * @param options - (Optional) Additional options for creating the JWS.
 * @returns A promise that resolves to a `SignedEvent` containing the JWS and linked block.
 *
 * @throws Will throw an error if the DID is not authenticated.
 *
 * @remarks
 * This function uses the DID's `createDagJWS` method to sign the payload and
 * returns the signed event, including the linked block as a `Uint8Array`.
 */
export async function signEvent(
  did: DID,
  payload: Record<string, unknown>,
  options?: CreateJWSOptions,
): Promise<SignedEvent> {
  if (!did.authenticated) {
    await did.authenticate()
  }
  const { linkedBlock, ...rest } = await did.createDagJWS(payload, options)
  return { ...rest, linkedBlock: new Uint8Array(linkedBlock) }
}

/**
 * A partial version of the initialization event header, with optional controllers.
 *
 * @remarks
 * This type is used to allow the creation of initialization events without requiring
 * the `controllers` field, as it will be injected automatically during the signing process.
 */
export type PartialInitEventHeader = Omit<InitEventHeader, 'controllers'> & {
  controllers?: [DIDString]
}

/**
 * Creates a signed initialization event using the provided DID, data, and header.
 *
 * @param did - The DID instance used to sign the initialization event.
 * @param data - The initialization data to be included in the event.
 * @param header - The header for the initialization event, with optional controllers.
 * @param options - (Optional) Additional options for creating the JWS.
 * @returns A promise that resolves to a `SignedEvent` representing the initialization event.
 *
 * @throws Will throw an error if the DID is not authenticated.
 *
 * @remarks
 * - If `controllers` are not provided in the header, they will be automatically set
 *   based on the DID's parent (if available) or the DID itself.
 * - The payload is encoded as an `InitEventPayload` before signing.
 */
export async function createSignedInitEvent<T>(
  did: DID,
  data: T,
  header: PartialInitEventHeader,
  options?: CreateJWSOptions,
): Promise<SignedEvent> {
  if (!did.authenticated) {
    await did.authenticate()
  }
  const controllers = header.controllers ?? [
    asDIDString(did.hasParent ? did.parent : did.id),
  ]
  const payload = InitEventPayload.encode({
    data,
    header: { ...header, controllers },
  })
  return await signEvent(did, payload, options)
}

/**
 * Decodes the payload of a signed event using the provided decoder.
 *
 * @param decoder - The decoder used to interpret the event's payload.
 * @param event - The signed event containing the payload to decode.
 * @returns A promise that resolves to the decoded payload.
 *
 * @throws Will throw an error if the linked block CID is missing or if decoding fails.
 *
 * @remarks
 * - The function reconstructs the block from the event's linked block and CID,
 *   using the DAG-CBOR codec and SHA-256 hasher.
 * - The payload is decoded using the provided decoder to ensure its validity and type safety.
 */
export async function getSignedEventPayload<Payload = Record<string, unknown>>(
  decoder: Decoder<unknown, Payload>,
  event: SignedEvent,
): Promise<Payload> {
  const cid = event.jws.link
  if (cid == null) {
    throw new Error('Missing linked block CID')
  }
  const block = await Block.create({
    bytes: event.linkedBlock,
    cid,
    codec: dagCbor,
    hasher: sha256,
  })
  return decode(decoder, block.value)
}
