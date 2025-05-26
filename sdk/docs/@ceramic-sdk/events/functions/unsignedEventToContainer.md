[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / unsignedEventToContainer

# Function: unsignedEventToContainer()

> **unsignedEventToContainer**\<`Payload`\>(`codec`, `event`): [`UnsignedEventContainer`](../type-aliases/UnsignedEventContainer.md)\<`Payload`\>

Decodes an unsigned Ceramic event into a container.

## Type Parameters

• **Payload**

## Parameters

• **codec**: `Decoder`\<`unknown`, `Payload`\>

The codec used to decode the event's payload.

• **event**: `unknown`

The unsigned Ceramic event to decode.

## Returns

[`UnsignedEventContainer`](../type-aliases/UnsignedEventContainer.md)\<`Payload`\>

An `UnsignedEventContainer` containing the decoded payload.

## Remarks

- This function assumes that the event is unsigned and decodes it accordingly.
- Use `eventToContainer` if the event type (signed or unsigned) is unknown.
