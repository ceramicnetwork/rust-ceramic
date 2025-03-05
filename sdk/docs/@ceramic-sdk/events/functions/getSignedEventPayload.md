[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / getSignedEventPayload

# Function: getSignedEventPayload()

> **getSignedEventPayload**\<`Payload`\>(`decoder`, `event`): `Promise`\<`Payload`\>

Decodes the payload of a signed event using the provided decoder.

## Type Parameters

• **Payload** = `Record`\<`string`, `unknown`\>

## Parameters

• **decoder**: `Decoder`\<`unknown`, `Payload`\>

The decoder used to interpret the event's payload.

• **event**: `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>

The signed event containing the payload to decode.

## Returns

`Promise`\<`Payload`\>

A promise that resolves to the decoded payload.

## Throws

Will throw an error if the linked block CID is missing or if decoding fails.

## Remarks

- The function reconstructs the block from the event's linked block and CID,
  using the DAG-CBOR codec and SHA-256 hasher.
- The payload is decoded using the provided decoder to ensure its validity and type safety.
