[**@ceramic-sdk/model-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-client](../README.md) / UpdateModelDefinitionParams

# Type Alias: UpdateModelDefinitionParams

> **UpdateModelDefinitionParams**: `object`

Parameters required to update a model stream.

## Type declaration

### controller?

> `optional` **controller**: `DID`

Optional `DID` instance for signing the model definition. *

### currentState?

> `optional` **currentState**: [`StreamState`](../../stream-client/type-aliases/StreamState.md)

Current model definition if known containing the stream's current state

### newContent

> **newContent**: [`ModelDefinition`](../../model-protocol/type-aliases/ModelDefinition.md)

New JSON object content for the stream, used with `currentContent` to create a JSON patch

### streamID

> **streamID**: `string`

String representation of the StreamID to update
