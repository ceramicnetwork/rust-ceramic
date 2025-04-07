[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / eventFromString

# Function: eventFromString()

> **eventFromString**\<`Payload`\>(`decoder`, `value`, `base`?): [`SignedEvent`](../type-aliases/SignedEvent.md) \| `Payload`

Decodes an event from a string using the specified decoder and base encoding.

## Type Parameters

• **Payload** = `unknown`

## Parameters

• **decoder**: `Decoder`\<`unknown`, `Payload`\>

The decoder to use for unsigned events.

• **value**: `string`

The string-encoded CAR containing the event.

• **base?**: `"base64url"` \| `"base256emoji"` \| `"base64"` \| `"base64pad"` \| `"base64urlpad"` \| `"base58btc"` \| `"base58flickr"` \| `"base36"` \| `"base36upper"` \| `"base32"` \| `"base32upper"` \| `"base32pad"` \| `"base32padupper"` \| `"base32hex"` \| `"base32hexupper"` \| `"base32hexpad"` \| `"base32hexpadupper"` \| `"base32z"` \| `"base16"` \| `"base16upper"` \| `"base10"` \| `"base8"` \| `"base2"` \| `"identity"`

The base encoding used (defaults to Base64).

## Returns

[`SignedEvent`](../type-aliases/SignedEvent.md) \| `Payload`

The decoded event, either a `SignedEvent` or a custom payload.
