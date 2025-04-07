[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / decodeMultibaseToJSON

# Function: decodeMultibaseToJSON()

> **decodeMultibaseToJSON**\<`T`\>(`value`): `T`

Decodes a multibase-encoded string into a JSON object.

## Type Parameters

• **T** = `Record`\<`string`, `unknown`\>

The expected shape of the returned JSON object (defaults to `Record<string, unknown>`).

## Parameters

• **value**: `string`

The multibase-encoded string to decode.

## Returns

`T`

The decoded JSON object.

## Example

```typescript
const json = decodeMultibaseToJSON<{ key: string }>('z1a2b3c...');
console.log(json.key); // Output: "value"
```
