[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / InitEventPayload

# Variable: InitEventPayload

> `const` **InitEventPayload**: `SparseCodec`\<`object`\>

Payload structure of Init events

## Type declaration

### data

> **data**: `TrivialCodec`\<`unknown`\> = `unknown`

### header

> **header**: `SparseCodec`\<`object`\> = `InitEventHeader`

#### Type declaration

##### context

> **context**: `OptionalCodec`\<`Type`\<[`StreamID`](../../identifiers/classes/StreamID.md), `Uint8Array`, [`StreamID`](../../identifiers/classes/StreamID.md) \| `Uint8Array`\>\>

##### controllers

> **controllers**: `TupleCodec`\<[`RefinementCodec`\<`TrivialCodec`\<`string`\>, `string` & `WithOpaque`\<`"DIDString"`\>\>]\>

##### model

> **model**: `Type`\<[`StreamID`](../../identifiers/classes/StreamID.md), `Uint8Array`, [`StreamID`](../../identifiers/classes/StreamID.md) \| `Uint8Array`\> = `streamIDAsBytes`

##### sep

> **sep**: `TrivialCodec`\<`string`\> = `string`

##### shouldIndex

> **shouldIndex**: `OptionalCodec`\<`TrivialCodec`\<`boolean`\>\>

##### unique

> **unique**: `OptionalCodec`\<`TrivialCodec`\<`Uint8Array`\>\>
