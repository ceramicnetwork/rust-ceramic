[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / createSignedInitEvent

# Function: createSignedInitEvent()

> **createSignedInitEvent**\<`T`\>(`did`, `data`, `header`, `options`?): `Promise`\<[`SignedEvent`](../type-aliases/SignedEvent.md)\>

Creates a signed initialization event using the provided DID, data, and header.

## Type Parameters

• **T**

## Parameters

• **did**: `DID`

The DID instance used to sign the initialization event.

• **data**: `T`

The initialization data to be included in the event.

• **header**: [`PartialInitEventHeader`](../type-aliases/PartialInitEventHeader.md)

The header for the initialization event, with optional controllers.

• **options?**: `CreateJWSOptions`

(Optional) Additional options for creating the JWS.

## Returns

`Promise`\<[`SignedEvent`](../type-aliases/SignedEvent.md)\>

A promise that resolves to a `SignedEvent` representing the initialization event.

## Throws

Will throw an error if the DID is not authenticated.

## Remarks

- If `controllers` are not provided in the header, they will be automatically set
  based on the DID's parent (if available) or the DID itself.
- The payload is encoded as an `InitEventPayload` before signing.
