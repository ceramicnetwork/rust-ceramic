import { randomStreamID } from '@ceramic-sdk/identifiers'
import { asDIDString } from '@didtools/codecs'
import { getAuthenticatedDID } from '@didtools/key-did'

import {
  InitEventPayload,
  SignedEvent,
  assertSignedEvent,
} from '../src/codecs.js'
import {
  encodeEventToCAR,
  eventFromCAR,
  eventFromString,
  eventToCAR,
  eventToString,
  signedEventToCAR,
} from '../src/encoding.js'
import { signEvent } from '../src/signing.js'

const did = await getAuthenticatedDID(new Uint8Array(32))

const testEventPayload: InitEventPayload = {
  data: null,
  header: {
    controllers: [asDIDString(did.id)],
    model: randomStreamID(),
    sep: 'test',
  },
}

const encodedTestPayload = InitEventPayload.encode(testEventPayload)

test('encode and decode unsigned init event as CAR', async () => {
  const encoded = encodeEventToCAR(InitEventPayload, testEventPayload)
  const decoded = eventFromCAR(InitEventPayload, encoded)
  expect(decoded).toEqual(testEventPayload)
})

test('encode and decode signed event as CAR', async () => {
  const event = await signEvent(did, encodedTestPayload)
  assertSignedEvent(event)
  const encoded = signedEventToCAR(event)
  const decoded = eventFromCAR(InitEventPayload, encoded)
  assertSignedEvent(decoded)
  expect(decoded).toEqual(event)
})

test('encode and decode any supported event as CAR', async () => {
  const signedEvent = await signEvent(did, encodedTestPayload)
  const encodedSignedEvent = eventToCAR(InitEventPayload, signedEvent)
  const decodedSignedEvent = eventFromCAR(InitEventPayload, encodedSignedEvent)
  assertSignedEvent(decodedSignedEvent)
  expect(decodedSignedEvent).toEqual(signedEvent)

  const encodedEvent = eventToCAR(InitEventPayload, testEventPayload)
  const decodedEvent = eventFromCAR(InitEventPayload, encodedEvent)
  expect(SignedEvent.is(decodedEvent)).toBe(false)
  expect(decodedEvent).toEqual(testEventPayload)
})

test('encode and decode any supported event as string', async () => {
  const signedEvent = await signEvent(did, encodedTestPayload)
  const encodedSignedEvent = eventToString(InitEventPayload, signedEvent)
  const decodedSignedEvent = eventFromString(
    InitEventPayload,
    encodedSignedEvent,
  )
  assertSignedEvent(decodedSignedEvent)
  expect(decodedSignedEvent).toEqual(signedEvent)

  const encodedEvent = eventToString(InitEventPayload, testEventPayload)
  const decodedEvent = eventFromString(InitEventPayload, encodedEvent)
  expect(SignedEvent.is(decodedEvent)).toBe(false)
  expect(decodedEvent).toEqual(testEventPayload)
})
