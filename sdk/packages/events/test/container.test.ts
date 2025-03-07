import { randomStreamID } from '@ceramic-sdk/identifiers'
import { asDIDString } from '@didtools/codecs'
import { getAuthenticatedDID } from '@didtools/key-did'

import { InitEventPayload } from '../src/codecs.js'
import {
  type SignedEventContainer,
  eventToContainer,
} from '../src/container.js'
import { signEvent } from '../src/signing.js'

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

const testEventPayload: InitEventPayload = {
  data: null,
  header: {
    controllers: [asDIDString(authenticatedDID.id)],
    model: randomStreamID(),
    sep: 'test',
  },
}

const encodedTestPayload = InitEventPayload.encode(testEventPayload)

test('eventToContainer() verifies the signed event signature and extract the payload', async () => {
  const signed = await signEvent(authenticatedDID, encodedTestPayload)
  const container = await eventToContainer(
    authenticatedDID,
    InitEventPayload,
    signed,
  )
  expect(container.signed).toBe(true)
  expect(
    (container as SignedEventContainer<InitEventPayload>).verified,
  ).toBeDefined()
  expect(container.payload).toEqual(testEventPayload)
})
