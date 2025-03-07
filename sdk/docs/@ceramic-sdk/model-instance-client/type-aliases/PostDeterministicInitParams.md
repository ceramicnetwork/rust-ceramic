[**@ceramic-sdk/model-instance-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / PostDeterministicInitParams

# Type Alias: PostDeterministicInitParams

> **PostDeterministicInitParams**: `object`

Parameters for posting a deterministic initialization event.

## Type declaration

### controller

> **controller**: `DIDString` \| `string`

The controller of the stream (DID string or literal string)

### model

> **model**: [`StreamID`](../../identifiers/classes/StreamID.md)

The model's stream ID

### uniqueValue?

> `optional` **uniqueValue**: `Uint8Array`

A unique value to ensure determinism of the event
