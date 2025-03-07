[**@ceramic-sdk/stream-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/stream-client](../README.md) / StreamState

# Type Alias: StreamState

> **StreamState**: `object`

## Type declaration

### controller

> **controller**: `string`

Controller of the stream

### data

> **data**: `string`

Multibase encoding of the data of the stream. Content is stream type specific

### dimensions

> **dimensions**: `Record`\<`string`, `string`\>

Dimensions of the stream, each value is multibase encoded

### event\_cid

> **event\_cid**: `string`

CID of the event that produced this state

### id

> **id**: `string`

Multibase encoding of the stream id
