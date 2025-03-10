[**@ceramic-sdk/model-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-client](../README.md) / createInitEvent

# Function: createInitEvent()

> **createInitEvent**(`did`, `data`): `Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

Creates a signed initialization event for a model using the provided DID and model definition.

## Parameters

• **did**: `DID`

The Decentralized Identifier (DID) to sign the initialization event.

• **data**: `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\> \| `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>

The model definition to be signed.

## Returns

`Promise`\<[`SignedEvent`](../../events/type-aliases/SignedEvent.md)\>

A promise that resolves to a `SignedEvent` representing the initialization event.

## Throws

Will throw an error if the model content is invalid or the DID is not authenticated.
