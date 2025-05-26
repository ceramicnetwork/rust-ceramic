[**@ceramic-sdk/model-instance-client v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / CreateDataEventParams

# Type Alias: CreateDataEventParams\<T\>

> **CreateDataEventParams**\<`T`\>: `object`

Parameters required to create a signed data event for a ModelInstanceDocument stream.

## Type Parameters

• **T** *extends* [`UnknownContent`](UnknownContent.md) = [`UnknownContent`](UnknownContent.md)

## Type declaration

### controller

> **controller**: `DID`

DID controlling the ModelInstanceDocument stream

### currentContent?

> `optional` **currentContent**: `T`

Current JSON object content for the stream, used with `newContent` to create a JSON patch

### currentID

> **currentID**: [`CommitID`](../../identifiers/classes/CommitID.md)

Commit ID of the current tip of the ModelInstanceDocument stream

### modelVersion?

> `optional` **modelVersion**: `CID`

CID of specific model version to use when validating this instance.
When empty the the init commit of the model is used

### newContent?

> `optional` **newContent**: `T`

New JSON object content for the stream, used with `currentContent` to create a JSON patch

### shouldIndex?

> `optional` **shouldIndex**: `boolean`

Flag indicating if indexers should index the stream
