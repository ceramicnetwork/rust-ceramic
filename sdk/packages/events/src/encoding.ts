import { DagJWS } from '@didtools/codecs'
import * as dagJson from '@ipld/dag-json'
import { type CAR, CARFactory, CarBlock } from 'cartonne'
import { type Codec, type Decoder, decode } from 'codeco'
import * as dagJose from 'dag-jose'
import { bases } from 'multiformats/basics'
import { CID } from 'multiformats/cid'
import { sha256 } from 'multihashes-sync/sha2'

import { SignedEvent } from './codecs.js'
import { base64urlToJSON, restrictBlockSize } from './utils.js'

// Initialize a CAR factory with support for DAG-JOSE and DAG-JSON codecs.
const carFactory = new CARFactory()
carFactory.codecs.add(dagJose)
carFactory.codecs.add(dagJson)
carFactory.hashers.add(sha256)

/** @internal */
export type Base = keyof typeof bases

/** Default base encoding for CAR files (Base64) */
export const DEFAULT_BASE: Base = 'base64'

/**
 * Encodes a CAR into a string using the specified base encoding.
 *
 * @param car - The CAR to encode.
 * @param base - The base encoding to use (defaults to Base64).
 * @returns The string-encoded CAR.
 *
 * @throws Will throw an error if the base encoding is not supported.
 */
export function carToString(car: CAR, base: Base = DEFAULT_BASE): string {
  return car.toString(base)
}

/**
 * Decodes a CAR from a string using the specified base encoding.
 *
 * @param value - The string-encoded CAR.
 * @param base - The base encoding used to decode the CAR (defaults to Base64).
 * @returns The decoded CAR object.
 *
 * @throws Will throw an error if the base encoding is not supported.
 */
export function carFromString(value: string, base: Base = DEFAULT_BASE): CAR {
  const codec = bases[base]
  if (codec == null) {
    throw new Error(`Unsupported base: ${base}`)
  }
  return carFactory.fromBytes(codec.decode(value))
}

/**
 * Encodes a signed event into a CAR format.
 *
 * @param event - The signed event to encode.
 * @returns A CAR object representing the signed event.
 *
 * @remarks
 * - Encodes the JWS, linked block, and optional `cacaoBlock` into the CAR.
 * - Validates block sizes using `restrictBlockSize` to ensure consistency.
 */
export function signedEventToCAR(event: SignedEvent): CAR {
  const { jws, linkedBlock, cacaoBlock } = event
  const car = carFactory.build()

  // if cacao is present, put it into ipfs dag
  if (cacaoBlock != null) {
    const header = base64urlToJSON<{ cap: string }>(jws.signatures[0].protected)
    const capCID = CID.parse(header.cap.replace('ipfs://', ''))
    car.blocks.put(new CarBlock(capCID, cacaoBlock))
    restrictBlockSize(cacaoBlock, capCID)
  }

  const payloadCID = jws.link
  if (payloadCID != null) {
    // Encode payload
    car.blocks.put(new CarBlock(payloadCID, linkedBlock))
    restrictBlockSize(linkedBlock, payloadCID)
  }

  // Encode JWS itself
  const cid = car.put(jws, {
    codec: 'dag-jose',
    hasher: 'sha2-256',
    isRoot: true,
  })
  // biome-ignore lint/style/noNonNullAssertion: added to CAR file right before
  const cidBlock = car.blocks.get(cid)!.payload
  restrictBlockSize(cidBlock, cid)

  return car
}

/**
 * Encodes an unsigned event into a CAR using the provided codec.
 *
 * @param codec - The codec used to encode the event.
 * @param event - The unsigned event to encode.
 * @returns A CAR object representing the unsigned event.
 *
 * @remarks
 * Encodes the event as the root of the CAR file using the specified codec.
 */
export function encodeEventToCAR(codec: Codec<unknown>, event: unknown): CAR {
  const car = carFactory.build()
  const cid = car.put(codec.encode(event), { isRoot: true })
  // biome-ignore lint/style/noNonNullAssertion: added to CAR file right before
  const cidBlock = car.blocks.get(cid)!.payload
  restrictBlockSize(cidBlock, cid)
  return car
}

/**
 * Encodes an event into a CAR. Supports both signed and unsigned events.
 *
 * @param codec - The codec used for unsigned events.
 * @param event - The event to encode (signed or unsigned).
 * @returns A CAR object representing the event.
 *
 * @remarks
 * Uses `signedEventToCAR` for signed events and `encodeEventToCAR` for unsigned events.
 */
export function eventToCAR(codec: Codec<unknown>, event: unknown): CAR {
  return SignedEvent.is(event)
    ? signedEventToCAR(event)
    : encodeEventToCAR(codec, event)
}

/**
 * Encodes an event into a string using the specified codec and base encoding.
 *
 * @param codec - The codec used for unsigned events.
 * @param event - The event to encode (signed or unsigned).
 * @param base - The base encoding to use (defaults to Base64).
 * @returns The string-encoded CAR representing the event.
 */
export function eventToString(
  codec: Codec<unknown>,
  event: unknown,
  base?: Base,
): string {
  return carToString(eventToCAR(codec, event), base)
}

/**
 * Decodes an event from a CAR object using the specified decoder.
 *
 * @param decoder - The decoder to use for unsigned events.
 * @param car - The CAR object containing the event.
 * @param eventCID - (Optional) The CID of the event to decode.
 * @returns The decoded event, either a `SignedEvent` or a custom payload.
 *
 * @throws Will throw an error if the linked block is missing or decoding fails.
 */
export function eventFromCAR<Payload = unknown>(
  decoder: Decoder<unknown, Payload>,
  car: CAR,
  eventCID?: CID,
): SignedEvent | Payload {
  const cid = eventCID ?? car.roots[0]
  const root = car.get(cid)

  if (DagJWS.is(root)) {
    const linkedBlock = root.link
      ? car.blocks.get(root.link)?.payload
      : undefined
    if (linkedBlock == null) {
      throw new Error('Linked block not found')
    }
    const event = decode(SignedEvent, { jws: root, linkedBlock })
    const header = base64urlToJSON<{ cap?: string }>(
      root.signatures[0].protected,
    )
    if (header.cap != null) {
      const capCID = CID.parse(header.cap.replace('ipfs://', ''))
      event.cacaoBlock = car.blocks.get(capCID)?.payload
    }
    return event
  }

  return decode(decoder, root)
}

/**
 * Decodes an event from a string using the specified decoder and base encoding.
 *
 * @param decoder - The decoder to use for unsigned events.
 * @param value - The string-encoded CAR containing the event.
 * @param base - The base encoding used (defaults to Base64).
 * @returns The decoded event, either a `SignedEvent` or a custom payload.
 */
export function eventFromString<Payload = unknown>(
  decoder: Decoder<unknown, Payload>,
  value: string,
  base?: Base,
): SignedEvent | Payload {
  return eventFromCAR(decoder, carFromString(value, base))
}
