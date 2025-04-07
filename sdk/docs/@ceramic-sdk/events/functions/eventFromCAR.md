[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / eventFromCAR

# Function: eventFromCAR()

> **eventFromCAR**\<`Payload`\>(`decoder`, `car`, `eventCID`?): [`SignedEvent`](../type-aliases/SignedEvent.md) \| `Payload`

Decodes an event from a CAR object using the specified decoder.

## Type Parameters

• **Payload** = `unknown`

## Parameters

• **decoder**: `Decoder`\<`unknown`, `Payload`\>

The decoder to use for unsigned events.

• **car**: `CAR`

The CAR object containing the event.

• **eventCID?**: `CID`\<`unknown`, `number`, `number`, `Version`\>

(Optional) The CID of the event to decode.

## Returns

[`SignedEvent`](../type-aliases/SignedEvent.md) \| `Payload`

The decoded event, either a `SignedEvent` or a custom payload.

## Throws

Will throw an error if the linked block is missing or decoding fails.
