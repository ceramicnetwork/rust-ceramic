[**@ceramic-sdk/model-instance-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / CreateInitEventParams

# Type Alias: CreateInitEventParams\<T\>

> **CreateInitEventParams**\<`T`\>: `object`

Parameters required to create a non-deterministic initialization event for a ModelInstanceDocument stream.

## Type Parameters

• **T** *extends* [`UnknownContent`](UnknownContent.md) = [`UnknownContent`](UnknownContent.md)

## Type declaration

### content

> **content**: `T`

Initial JSON object content for the ModelInstanceDocument stream

### context?

> `optional` **context**: [`StreamID`](../../identifiers/classes/StreamID.md)

Optional context

### controller

> **controller**: `DID`

DID controlling the ModelInstanceDocument stream

### model

> **model**: [`StreamID`](../../identifiers/classes/StreamID.md)

Stream ID of the Model used by the ModelInstanceDocument stream

### shouldIndex?

> `optional` **shouldIndex**: `boolean`

Flag indicating if indexers should index the ModelInstanceDocument stream (defaults to `true`)
