[**@ceramic-sdk/model-instance-client v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / createInitEvent

# Function: createInitEvent()

> **createInitEvent**\<`T`\>(`params`): `Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

Creates a non-deterministic initialization event for a ModelInstanceDocument stream.

## Type Parameters

• **T** *extends* [`UnknownContent`](../type-aliases/UnknownContent.md) = [`UnknownContent`](../type-aliases/UnknownContent.md)

## Parameters

• **params**: [`CreateInitEventParams`](../type-aliases/CreateInitEventParams.md)\<`T`\>

The parameters required to create the initialization event.

## Returns

`Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

A promise that resolves to a signed initialization event.

## Remarks

This method creates a non-deterministic initialization event for use with streams where
the stream ID is not derived from a unique value.

## See

[getDeterministicInitEventPayload](getDeterministicInitEventPayload.md) for deterministic initialization events.
