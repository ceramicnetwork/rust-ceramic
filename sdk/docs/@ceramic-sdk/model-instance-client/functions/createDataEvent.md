[**@ceramic-sdk/model-instance-client v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / createDataEvent

# Function: createDataEvent()

> **createDataEvent**\<`T`\>(`params`): `Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

Creates a signed data event for a ModelInstanceDocument stream.

## Type Parameters

• **T** *extends* [`UnknownContent`](../type-aliases/UnknownContent.md) = [`UnknownContent`](../type-aliases/UnknownContent.md)

## Parameters

• **params**: [`CreateDataEventParams`](../type-aliases/CreateDataEventParams.md)\<`T`\>

Parameters required to create the data event.

## Returns

`Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

A promise that resolves to the signed data event.

## Remarks

The data event updates the content of the stream by applying JSON patch operations
to the existing content. The resulting event is signed by the controlling DID.
