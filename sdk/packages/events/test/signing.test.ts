import { randomStreamID } from '@ceramic-sdk/identifiers'
import { asDIDString } from '@didtools/codecs'
import { createDID, getAuthenticatedDID } from '@didtools/key-did'

import {
  type InitEventHeader,
  InitEventPayload,
  assertSignedEvent,
} from '../src/codecs.js'
import {
  type PartialInitEventHeader,
  createSignedInitEvent,
  getSignedEventPayload,
  signEvent,
} from '../src/signing.js'

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

const defaultHeader: PartialInitEventHeader = {
  model: randomStreamID(),
  sep: 'model',
}

const testEventPayload: InitEventPayload = {
  data: null,
  header: {
    controllers: [asDIDString(authenticatedDID.id)],
    model: randomStreamID(),
    sep: 'test',
  },
}

const encodedTestPayload = InitEventPayload.encode(testEventPayload)

test('signEvent() signs the given event payload', async () => {
  const event = await signEvent(authenticatedDID, encodedTestPayload)
  assertSignedEvent(event)
})

test('getSignedEventPayload() returns the EventPayload of a SignedEvent', async () => {
  const signed = await signEvent(authenticatedDID, encodedTestPayload)
  const event = await getSignedEventPayload(InitEventPayload, signed)
  expect(event).toEqual(testEventPayload)
})

describe('createSignedInitEvent()', () => {
  test('authenticates the DID', async () => {
    await expect(async () => {
      await createSignedInitEvent(createDID(), null, defaultHeader)
    }).rejects.toThrow('No provider available')

    const event = await createSignedInitEvent(
      authenticatedDID,
      null,
      defaultHeader,
    )
    assertSignedEvent(event)
  })

  test('fills the header values', async () => {
    const event = await createSignedInitEvent(
      authenticatedDID,
      null,
      defaultHeader,
    )
    const payload = await getSignedEventPayload(InitEventPayload, event)
    expect(payload.header).toEqual({
      ...defaultHeader,
      controllers: [authenticatedDID.id],
    })
  })

  test('uses the provided values', async () => {
    const header: InitEventHeader = {
      controllers: [asDIDString('did:test:123')],
      model: randomStreamID(),
      sep: 'foo',
      unique: new Uint8Array([4, 5, 6]),
    }
    const event = await createSignedInitEvent(authenticatedDID, null, header)
    const payload = await getSignedEventPayload(InitEventPayload, event)
    expect(payload.header).toEqual(header)
  })
})
