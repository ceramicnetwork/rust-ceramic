[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / eventToContainer

# Function: eventToContainer()

> **eventToContainer**\<`Payload`\>(`did`, `codec`, `event`): `Promise`\<[`EventContainer`](../type-aliases/EventContainer.md)\<`Payload`\>\>

Decodes a Ceramic event (signed or unsigned) into a container.

## Type Parameters

• **Payload**

## Parameters

• **did**: `DID`

The DID used to verify the event's JWS if it is signed.

• **codec**: `Decoder`\<`unknown`, `Payload`\>

The codec used to decode the event's payload.

• **event**: `unknown`

The Ceramic event to decode (can be signed or unsigned).

## Returns

`Promise`\<[`EventContainer`](../type-aliases/EventContainer.md)\<`Payload`\>\>

A promise that resolves to an `EventContainer` containing the decoded payload and metadata.

## Remarks

- This function determines the type of the event (signed or unsigned) and processes it accordingly.
- For signed events, it verifies the JWS and decodes the payload.
- For unsigned events, it simply decodes the payload.
