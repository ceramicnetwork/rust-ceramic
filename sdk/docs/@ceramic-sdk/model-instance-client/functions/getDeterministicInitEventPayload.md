[**@ceramic-sdk/model-instance-client v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / getDeterministicInitEventPayload

# Function: getDeterministicInitEventPayload()

> **getDeterministicInitEventPayload**(`model`, `controller`, `uniqueValue`?): [`DeterministicInitEventPayload`](../../model-instance-protocol/type-aliases/DeterministicInitEventPayload.md)

Retrieves the payload for a deterministic initialization event for a ModelInstanceDocument stream.

## Parameters

• **model**: [`StreamID`](../../identifiers/classes/StreamID.md)

The stream ID of the model associated with the stream.

• **controller**: `string` \| `string` & `WithOpaque`\<`"DIDString"`\>

The DID string or literal string for the stream's controller.

• **uniqueValue?**: `Uint8Array`

Optional unique value to ensure determinism.

## Returns

[`DeterministicInitEventPayload`](../../model-instance-protocol/type-aliases/DeterministicInitEventPayload.md)

The deterministic initialization event payload.

## Remarks

Deterministic initialization events ensure the resulting stream ID is derived
from the provided unique value.
