[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / eventToCAR

# Function: eventToCAR()

> **eventToCAR**(`codec`, `event`): `CAR`

Encodes an event into a CAR. Supports both signed and unsigned events.

## Parameters

• **codec**: `Codec`\<`unknown`, `unknown`, `unknown`\>

The codec used for unsigned events.

• **event**: `unknown`

The event to encode (signed or unsigned).

## Returns

`CAR`

A CAR object representing the event.

## Remarks

Uses `signedEventToCAR` for signed events and `encodeEventToCAR` for unsigned events.
