[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / carFromString

# Function: carFromString()

> **carFromString**(`value`, `base`): `CAR`

Decodes a CAR from a string using the specified base encoding.

## Parameters

• **value**: `string`

The string-encoded CAR.

• **base**: `"base64url"` \| `"base256emoji"` \| `"base64"` \| `"base64pad"` \| `"base64urlpad"` \| `"base58btc"` \| `"base58flickr"` \| `"base36"` \| `"base36upper"` \| `"base32"` \| `"base32upper"` \| `"base32pad"` \| `"base32padupper"` \| `"base32hex"` \| `"base32hexupper"` \| `"base32hexpad"` \| `"base32hexpadupper"` \| `"base32z"` \| `"base16"` \| `"base16upper"` \| `"base10"` \| `"base8"` \| `"base2"` \| `"identity"` = `DEFAULT_BASE`

The base encoding used to decode the CAR (defaults to Base64).

## Returns

`CAR`

The decoded CAR object.

## Throws

Will throw an error if the base encoding is not supported.
