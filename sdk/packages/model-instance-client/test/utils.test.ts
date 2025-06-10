import { randomStreamID } from '@ceramic-sdk/identifiers'
import { getAuthenticatedDID } from '@didtools/key-did'
import { equals } from 'uint8arrays'

import { createInitHeader } from '../src/utils.js'

const authenticatedDID = await getAuthenticatedDID(new Uint8Array(32))

describe('createInitHeader()', () => {
  test('adds random unique bytes by default or when explcitly set to false', () => {
    const model = randomStreamID()

    const header1 = createInitHeader({ controller: authenticatedDID, model })
    expect(header1.unique).toBeInstanceOf(Uint8Array)
    const header2 = createInitHeader({ controller: authenticatedDID, model })
    expect(header2.unique).toBeInstanceOf(Uint8Array)
    expect(
      equals(header1.unique as Uint8Array, header2.unique as Uint8Array),
    ).toBe(false)

    const header3 = createInitHeader({
      controller: authenticatedDID,
      model,
      unique: false,
    })
    expect(header3.unique).toBeInstanceOf(Uint8Array)
    expect(
      equals(header1.unique as Uint8Array, header3.unique as Uint8Array),
    ).toBe(false)
  })

  test('adds the specified unique bytes', () => {
    const model = randomStreamID()
    const unique = new Uint8Array([0, 1, 2])

    const header1 = createInitHeader({
      controller: authenticatedDID,
      model,
      unique,
    })
    expect(header1.unique).toBeInstanceOf(Uint8Array)
    const header2 = createInitHeader({
      controller: authenticatedDID,
      model,
      unique,
    })
    expect(header2.unique).toBeInstanceOf(Uint8Array)

    expect(
      equals(header1.unique as Uint8Array, header2.unique as Uint8Array),
    ).toBe(true)
  })

  test('does not add unique bytes if set to true', () => {
    const model = randomStreamID()
    const header = createInitHeader({
      controller: authenticatedDID,
      model,
      unique: true,
    })
    expect(header.unique).toBeUndefined()
  })

  test('does not add context and shouldIndex by default', () => {
    const model = randomStreamID()
    const header = createInitHeader({ controller: authenticatedDID, model })
    expect(header.context).toBeUndefined()
    expect(header.shouldIndex).toBeUndefined()
  })

  test('adds context and shouldIndex if specified', () => {
    const model = randomStreamID()
    const context = randomStreamID()
    const header = createInitHeader({
      controller: authenticatedDID,
      model,
      context,
      shouldIndex: true,
    })
    expect(header.context?.equals(context)).toBe(true)
    expect(header.shouldIndex).toBe(true)
  })
})
