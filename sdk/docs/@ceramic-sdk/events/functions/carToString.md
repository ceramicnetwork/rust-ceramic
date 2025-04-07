[**@ceramic-sdk/events v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / carToString

# Function: carToString()

> **carToString**(`car`, `base`): `string`

Encodes a CAR into a string using the specified base encoding.

## Parameters

• **car**: `CAR`

The CAR to encode.

• **base**: `"base64url"` \| `"base256emoji"` \| `"base64"` \| `"base64pad"` \| `"base64urlpad"` \| `"base58btc"` \| `"base58flickr"` \| `"base36"` \| `"base36upper"` \| `"base32"` \| `"base32upper"` \| `"base32pad"` \| `"base32padupper"` \| `"base32hex"` \| `"base32hexupper"` \| `"base32hexpad"` \| `"base32hexpadupper"` \| `"base32z"` \| `"base16"` \| `"base16upper"` \| `"base10"` \| `"base8"` \| `"base2"` \| `"identity"` = `DEFAULT_BASE`

The base encoding to use (defaults to Base64).

## Returns

`string`

The string-encoded CAR.

## Throws

Will throw an error if the base encoding is not supported.
