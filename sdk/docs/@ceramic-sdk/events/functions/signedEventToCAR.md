[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / signedEventToCAR

# Function: signedEventToCAR()

> **signedEventToCAR**(`event`): `CAR`

Encodes a signed event into a CAR format.

## Parameters

• **event**: `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>

The signed event to encode.

## Returns

`CAR`

A CAR object representing the signed event.

## Remarks

- Encodes the JWS, linked block, and optional `cacaoBlock` into the CAR.
- Validates block sizes using `restrictBlockSize` to ensure consistency.
