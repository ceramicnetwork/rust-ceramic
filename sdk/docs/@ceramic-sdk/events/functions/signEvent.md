[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / signEvent

# Function: signEvent()

> **signEvent**(`did`, `payload`, `options`?): `Promise`\<[`SignedEvent`](../type-aliases/SignedEvent.md)\>

Signs an event payload using the provided DID.

## Parameters

• **did**: `DID`

The DID instance used to sign the payload.

• **payload**: `Record`\<`string`, `unknown`\>

The data to be signed, provided as a key-value map.

• **options?**: `CreateJWSOptions`

(Optional) Additional options for creating the JWS.

## Returns

`Promise`\<[`SignedEvent`](../type-aliases/SignedEvent.md)\>

A promise that resolves to a `SignedEvent` containing the JWS and linked block.

## Throws

Will throw an error if the DID is not authenticated.

## Remarks

This function uses the DID's `createDagJWS` method to sign the payload and
returns the signed event, including the linked block as a `Uint8Array`.
