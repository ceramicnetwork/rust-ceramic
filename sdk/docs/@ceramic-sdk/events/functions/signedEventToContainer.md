[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / signedEventToContainer

# Function: signedEventToContainer()

> **signedEventToContainer**\<`Payload`\>(`did`, `codec`, `event`): `Promise`\<[`SignedEventContainer`](../type-aliases/SignedEventContainer.md)\<`Payload`\>\>

Decodes a signed Ceramic event into a container.

## Type Parameters

• **Payload**

## Parameters

• **did**: `DID`

The DID used to verify the event's JWS.

• **codec**: `Decoder`\<`unknown`, `Payload`\>

The codec used to decode the event's payload.

• **event**: `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>

The signed Ceramic event to decode.

## Returns

`Promise`\<[`SignedEventContainer`](../type-aliases/SignedEventContainer.md)\<`Payload`\>\>

A promise that resolves to a `SignedEventContainer` containing the decoded payload and metadata.

## Throws

Will throw an error if the linked block CID is missing or if verification fails.

## Remarks

- This function verifies the event's JWS and decodes its payload simultaneously.
- It also includes additional metadata such as the verification result and `cacaoBlock` if present.
