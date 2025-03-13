[**@ceramic-sdk/model-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-client](../README.md) / createDataEventPayload

# Function: createDataEventPayload()

> **createDataEventPayload**(`current`, `data`): [`ModelDataEventPayload`](../../model-protocol/type-aliases/ModelDataEventPayload.md)

Creates a data event payload for a Model stream.

## Parameters

• **current**: [`CommitID`](../../identifiers/classes/CommitID.md)

The current commit ID of the stream.

• **data**: (`MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\> \| `MapIn`\<`object`, `$TypeOf`\>)[]

The JSON patch operations to apply to the stream content.

## Returns

[`ModelDataEventPayload`](../../model-protocol/type-aliases/ModelDataEventPayload.md)

A valid data event payload.

## Throws

Will throw an error if the JSON patch operations are invalid.
