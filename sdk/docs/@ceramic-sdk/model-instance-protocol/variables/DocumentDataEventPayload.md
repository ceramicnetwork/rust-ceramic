[**@ceramic-sdk/model-instance-protocol v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-protocol](../README.md) / DocumentDataEventPayload

# Variable: DocumentDataEventPayload

> `const` **DocumentDataEventPayload**: `SparseCodec`\<`object`\>

Data event payload for a ModelInstanceDocument Stream

## Type declaration

### data

> **data**: `Codec`\<(`MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\>)[], (`MapIn`\<`object`, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\>)[], `unknown`\> & `object`

### header

> **header**: `OptionalCodec`\<`SparseCodec`\<`object`\>\>

### id

> **id**: `Type`\<`CID`\<`unknown`, `number`, `number`, `Version`\>, `CID`\<`unknown`, `number`, `number`, `Version`\>, `unknown`\> = `cid`

### prev

> **prev**: `Type`\<`CID`\<`unknown`, `number`, `number`, `Version`\>, `CID`\<`unknown`, `number`, `number`, `Version`\>, `unknown`\> = `cid`
