import { type Right, isRight, validate } from 'codeco'

import {
  isStreamIDString,
  streamIDAsString,
  streamIDString,
} from '../src/codecs.js'
import type { StreamID } from '../src/stream-id.js'
import { randomCID, randomStreamID } from '../src/utils.js'

describe('isStreamIDString', () => {
  test('ok', () => {
    expect(isStreamIDString(randomStreamID().toString())).toBe(true)
  })
  test('not ok', () => {
    expect(isStreamIDString(randomCID().toString())).toBe(false)
  })
})

describe('streamIDString', () => {
  const streamId = randomStreamID().toString()
  test('decode: ok', () => {
    const result = validate(streamIDString, streamId)
    expect(isRight(result)).toEqual(true)
    expect((result as unknown as Right<StreamID>).right).toBe(streamId)
  })
  test('decode: not ok', () => {
    const result = validate(streamIDString, 'garbage')
    expect(isRight(result)).toEqual(false)
  })
  test('encode', () => {
    const result = streamIDString.encode(streamId)
    expect(result).toBe(streamId)
  })
})

describe('streamIdAsString', () => {
  const streamID = randomStreamID()
  test('decode: ok', () => {
    const result = validate(streamIDAsString, streamID.toString())
    expect(isRight(result)).toEqual(true)
    expect((result as Right<StreamID>).right).toEqual(streamID)
  })
  test('decode: not ok', () => {
    const result = validate(streamIDAsString, 'garbage')
    expect(isRight(result)).toEqual(false)
  })
  test('encode', () => {
    const result = streamIDAsString.encode(streamID)
    expect(result).toEqual(streamID.toString())
  })
})
