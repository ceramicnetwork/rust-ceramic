[**@ceramic-sdk/model-instance-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / createDataEventPayload

# Function: createDataEventPayload()

> **createDataEventPayload**(`current`, `data`, `header`?): [`DocumentDataEventPayload`](../../model-instance-protocol/type-aliases/DocumentDataEventPayload.md)

Creates a data event payload for a ModelInstanceDocument stream.

## Parameters

• **current**: [`CommitID`](../../identifiers/classes/CommitID.md)

The current commit ID of the stream.

• **data**: (`MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\>)[]

The JSON patch operations to apply to the stream content.

• **header?**: `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>

Optional header information for the data event.

## Returns

[`DocumentDataEventPayload`](../../model-instance-protocol/type-aliases/DocumentDataEventPayload.md)

A valid data event payload.

## Throws

Will throw an error if the JSON patch operations are invalid.
