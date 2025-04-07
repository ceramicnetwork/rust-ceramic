[**@ceramic-sdk/model-instance-protocol v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-protocol](../README.md) / DataInitEventPayload

# Variable: DataInitEventPayload

> `const` **DataInitEventPayload**: `SparseCodec`\<`object`\>

Init event payload for a non-deterministic ModelInstanceDocument Stream

## Type declaration

### data

> **data**: `TrivialCodec`\<`Record`\<`string`, `unknown`\>\> = `unknownRecord`

### header

> **header**: `SparseCodec`\<`object`\> = `DocumentInitEventHeader`

#### Type declaration

##### context

> **context**: `OptionalCodec`\<`Type`\<[`StreamID`](../../identifiers/classes/StreamID.md), `Uint8Array`, [`StreamID`](../../identifiers/classes/StreamID.md) \| `Uint8Array`\>\>

##### controllers

> **controllers**: `TupleCodec`\<[`RefinementCodec`\<`TrivialCodec`\<`string`\>, `string` & `WithOpaque`\<`"DIDString"`\>\>]\>

##### model

> **model**: `Type`\<[`StreamID`](../../identifiers/classes/StreamID.md), `Uint8Array`, [`StreamID`](../../identifiers/classes/StreamID.md) \| `Uint8Array`\> = `streamIDAsBytes`

##### modelVersion

> **modelVersion**: `OptionalCodec`\<`Type`\<`CID`\<`unknown`, `number`, `number`, `Version`\>, `CID`\<`unknown`, `number`, `number`, `Version`\>, `unknown`\>\>

##### sep

> **sep**: `LiteralCodec`\<`"model"`\>

##### shouldIndex

> **shouldIndex**: `OptionalCodec`\<`TrivialCodec`\<`boolean`\>\>

##### unique

> **unique**: `OptionalCodec`\<`TrivialCodec`\<`Uint8Array`\>\>
