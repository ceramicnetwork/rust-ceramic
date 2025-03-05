export const STREAMID_CODEC = 206

export const STREAM_TYPES = {
  tile: 0,
  'caip10-link': 1,
  model: 2,
  MID: 3,
  UNLOADABLE: 4,
} as const satisfies Record<string, number>

type StreamTypes = typeof STREAM_TYPES

export type StreamTypeName = keyof StreamTypes

export type StreamTypeCode = StreamTypes[StreamTypeName]

export type StreamType = StreamTypeCode | StreamTypeName
