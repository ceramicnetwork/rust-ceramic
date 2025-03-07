[**@ceramic-sdk/stream-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/stream-client](../README.md) / StreamClient

# Class: StreamClient

Represents a Stream Client.

This class provides basic utility methods to interact with streams, including
accessing the Ceramic client and obtaining stream states. It also manages a
Decentralized Identifier (DID) that can be used for authentication or signing.

This class is intended to be extended by other classes for more specific functionalities.

## Extended by

## Constructors

### new StreamClient()

> **new StreamClient**(`params`): [`StreamClient`](StreamClient.md)

Creates a new instance of `StreamClient`.

#### Parameters

• **params**: [`StreamClientParams`](../type-aliases/StreamClientParams.md)

Configuration object containing parameters for initializing the StreamClient.

#### Returns

[`StreamClient`](StreamClient.md)

## Accessors

### ceramic

> `get` **ceramic**(): [`CeramicClient`](../../http-client/classes/CeramicClient.md)

Retrieves the Ceramic HTTP client instance used to interact with the Ceramic server.

This client is essential for executing API requests to fetch stream data.

#### Returns

[`CeramicClient`](../../http-client/classes/CeramicClient.md)

The `CeramicClient` instance associated with this StreamClient.

#### Defined in

## Methods

### getDID()

> **getDID**(`provided`?): `DID`

Retrieves a Decentralized Identifier (DID).

This method provides access to a DID, which is required for authentication or event signing operations.
The caller can optionally provide a DID; if none is provided, the instance's DID is used instead.

#### Parameters

• **provided?**: `DID`

An optional DID object to use. If not supplied, the instance's DID is returned.

#### Returns

`DID`

The DID object, either provided or attached to the instance.

#### Throws

Will throw an error if neither a provided DID nor an instance DID is available.

#### Example

```typescript
const did = client.getDID();
console.log(did.id); // Outputs the DID identifier
```

***

### getStreamState()

> **getStreamState**(`streamId`): `Promise`\<[`StreamState`](../type-aliases/StreamState.md)\>

Fetches the current stream state of a specific stream by its ID.

This method interacts with the Ceramic HTTP API to retrieve the state of a stream.
The stream ID can either be a multibase-encoded string or a `StreamID` object.

#### Parameters

• **streamId**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The unique identifier of the stream to fetch.
  - Can be a multibase-encoded string or a `StreamID` object.

#### Returns

`Promise`\<[`StreamState`](../type-aliases/StreamState.md)\>

A Promise that resolves to the `StreamState` object, representing the current state of the stream.

#### Throws

Will throw an error if the API request fails or returns an error response (if stream is not found).

#### Example

```typescript
const streamState = await client.getStreamState('kjzl6cwe1...');
console.log(streamState);
```
