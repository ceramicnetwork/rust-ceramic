[**@ceramic-sdk/model-instance-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / getDeterministicInitEvent

# Function: getDeterministicInitEvent()

> **getDeterministicInitEvent**(`model`, `controller`, `uniqueValue`?): [`EncodedDeterministicInitEventPayload`](../../model-instance-protocol/type-aliases/EncodedDeterministicInitEventPayload.md)

Encodes a deterministic initialization event for a ModelInstanceDocument stream.

## Parameters

• **model**: [`StreamID`](../../identifiers/classes/StreamID.md)

The stream ID of the model associated with the stream.

• **controller**: `string` \| `string` & `WithOpaque`\<`"DIDString"`\>

The DID string or literal string for the stream's controller.

• **uniqueValue?**: `Uint8Array`

Optional unique value to ensure determinism.

## Returns

[`EncodedDeterministicInitEventPayload`](../../model-instance-protocol/type-aliases/EncodedDeterministicInitEventPayload.md)

The encoded deterministic initialization event payload.
