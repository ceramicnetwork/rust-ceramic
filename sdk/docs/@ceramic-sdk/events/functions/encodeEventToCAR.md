[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / encodeEventToCAR

# Function: encodeEventToCAR()

> **encodeEventToCAR**(`codec`, `event`): `CAR`

Encodes an unsigned event into a CAR using the provided codec.

## Parameters

• **codec**: `Codec`\<`unknown`, `unknown`, `unknown`\>

The codec used to encode the event.

• **event**: `unknown`

The unsigned event to encode.

## Returns

`CAR`

A CAR object representing the unsigned event.

## Remarks

Encodes the event as the root of the CAR file using the specified codec.
