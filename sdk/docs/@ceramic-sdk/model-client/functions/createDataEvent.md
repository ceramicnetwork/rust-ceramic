[**@ceramic-sdk/model-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-client](../README.md) / createDataEvent

# Function: createDataEvent()

> **createDataEvent**(`params`): `Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

Creates a signed data event for a Model stream.

## Parameters

• **params**: [`CreateDataEventParams`](../type-aliases/CreateDataEventParams.md)

Parameters required to create the data event.

## Returns

`Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

A promise that resolves to the signed data event.

## Remarks

The data event updates the content of the stream by applying JSON patch operations
to the existing content. The resulting event is signed by the controlling DID.
