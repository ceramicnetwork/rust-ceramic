[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / SignedEventContainer

# Type Alias: SignedEventContainer\<Payload\>

> **SignedEventContainer**\<`Payload`\>: `object`

A container for a signed Ceramic event.

## Type Parameters

• **Payload**

The type of the event's payload.

## Type declaration

### cacaoBlock?

> `optional` **cacaoBlock**: `Uint8Array`

### cid

> **cid**: `CID`

### payload

> **payload**: `Payload`

### signed

> **signed**: `true`

### verified

> **verified**: `VerifyJWSResult`
