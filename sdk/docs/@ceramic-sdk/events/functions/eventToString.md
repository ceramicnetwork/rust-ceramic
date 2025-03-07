[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / eventToString

# Function: eventToString()

> **eventToString**(`codec`, `event`, `base`?): `string`

Encodes an event into a string using the specified codec and base encoding.

## Parameters

• **codec**: `Codec`\<`unknown`, `unknown`, `unknown`\>

The codec used for unsigned events.

• **event**: `unknown`

The event to encode (signed or unsigned).

• **base?**: `"base64url"` \| `"base256emoji"` \| `"base64"` \| `"base64pad"` \| `"base64urlpad"` \| `"base58btc"` \| `"base58flickr"` \| `"base36"` \| `"base36upper"` \| `"base32"` \| `"base32upper"` \| `"base32pad"` \| `"base32padupper"` \| `"base32hex"` \| `"base32hexupper"` \| `"base32hexpad"` \| `"base32hexpadupper"` \| `"base32z"` \| `"base16"` \| `"base16upper"` \| `"base10"` \| `"base8"` \| `"base2"` \| `"identity"`

The base encoding to use (defaults to Base64).

## Returns

`string`

The string-encoded CAR representing the event.
