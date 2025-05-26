[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / decodeMultibaseToStreamID

# Function: decodeMultibaseToStreamID()

> **decodeMultibaseToStreamID**(`value`): [`StreamID`](../../identifiers/classes/StreamID.md)

Decodes a multibase-encoded string into a `StreamID`.

## Parameters

• **value**: `string`

The multibase-encoded string to decode.

## Returns

[`StreamID`](../../identifiers/classes/StreamID.md)

A `StreamID` object representing the decoded value.

## Example

```typescript
const streamID = decodeMultibaseToStreamID('z1a2b3c...');
console.log(streamID.toString()); // Output: StreamID
```
