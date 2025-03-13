[**@ceramic-sdk/model-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-client](../README.md) / CreateDataEventParams

# Type Alias: CreateDataEventParams

> **CreateDataEventParams**: `object`

Parameters required to create a signed data event for a Model stream.

## Type declaration

### controller

> **controller**: `DID`

DID controlling the ModelInstanceDocument stream

### currentContent?

> `optional` **currentContent**: [`ModelDefinition`](../../model-protocol/type-aliases/ModelDefinition.md)

Current JSON object content for the stream, used with `newContent` to create a JSON patch

### currentID

> **currentID**: [`CommitID`](../../identifiers/classes/CommitID.md)

Commit ID of the current tip of the ModelInstanceDocument stream

### newContent?

> `optional` **newContent**: [`ModelDefinition`](../../model-protocol/type-aliases/ModelDefinition.md)

New JSON object content for the stream, used with `currentContent` to create a JSON patch
