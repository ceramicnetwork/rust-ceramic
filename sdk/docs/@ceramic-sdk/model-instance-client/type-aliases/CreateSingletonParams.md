[**@ceramic-sdk/model-instance-client v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / CreateSingletonParams

# Type Alias: CreateSingletonParams

> **CreateSingletonParams**: `object`

Parameters for creating a singleton instance of a model.

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
