[**@ceramic-sdk/model-instance-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-instance-client](../README.md) / ModelInstanceClient

# Class: ModelInstanceClient

Extends the StreamClient to add functionality for interacting with Ceramic model instance documents.

The `ModelInstanceClient` class provides methods to:
- Retrieve events and document states
- Post deterministic and signed initialization events
- Update existing documents with new content

## Extends

- [`StreamClient`](../../stream-client/classes/StreamClient.md)

## Constructors

### new ModelInstanceClient()

> **new ModelInstanceClient**(`params`): [`ModelInstanceClient`](ModelInstanceClient.md)

Creates a new instance of `StreamClient`.

#### Parameters

• **params**: [`StreamClientParams`](../../stream-client/type-aliases/StreamClientParams.md)

Configuration object containing parameters for initializing the StreamClient.

#### Returns

[`ModelInstanceClient`](ModelInstanceClient.md)

#### Inherited from

[`StreamClient`](../../stream-client/classes/StreamClient.md).[`constructor`](../../stream-client/classes/StreamClient.md#constructors)

## Accessors

### ceramic

> `get` **ceramic**(): [`CeramicClient`](../../http-client/classes/CeramicClient.md)

Retrieves the Ceramic HTTP client instance used to interact with the Ceramic server.

This client is essential for executing API requests to fetch stream data.

#### Returns

[`CeramicClient`](../../http-client/classes/CeramicClient.md)

The `CeramicClient` instance associated with this StreamClient.

#### Inherited from

[`StreamClient`](../../stream-client/classes/StreamClient.md).[`ceramic`](../../stream-client/classes/StreamClient.md#ceramic)

#### Defined in

## Methods

### getCurrentID()

> **getCurrentID**(`streamID`): [`CommitID`](../../identifiers/classes/CommitID.md)

Retrieves the `CommitID` for the provided stream ID.

#### Parameters

• **streamID**: `string`

The stream ID string.

#### Returns

[`CommitID`](../../identifiers/classes/CommitID.md)

The `CommitID` for the stream.

***

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

#### Inherited from

[`StreamClient`](../../stream-client/classes/StreamClient.md).[`getDID`](../../stream-client/classes/StreamClient.md#getdid)

***

### getDocumentState()

> **getDocumentState**(`streamID`): `Promise`\<`DocumentState`\>

Retrieves the document state for a given stream ID.

#### Parameters

• **streamID**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The stream ID, either as a `StreamID` object or string.

#### Returns

`Promise`\<`DocumentState`\>

A promise that resolves to the `DocumentState`.

#### Throws

Will throw an error if the stream ID is invalid or the request fails.

#### Remarks

This method fetches the stream state using the extended StreamClient's `getStreamState` method.

***

### getEvent()

> **getEvent**(`commitID`): `Promise`\<`MapIn`\<`RequiredProps`\<`object`\>, `$OutputOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$OutputOf`\> \| `MapIn`\<`RequiredProps`\<`object`\>, `$OutputOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\>\>

Retrieves a `DocumentEvent` based on its commit ID.

#### Parameters

• **commitID**: `string` \| [`CommitID`](../../identifiers/classes/CommitID.md)

The commit ID of the event, either as a `CommitID` object or string.

#### Returns

`Promise`\<`MapIn`\<`RequiredProps`\<`object`\>, `$OutputOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$OutputOf`\> \| `MapIn`\<`RequiredProps`\<`object`\>, `$OutputOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$OutputOf`\> \| `MapIn`\<`object`, `$OutputOf`\>\>

A promise that resolves to the `DocumentEvent` for the specified commit ID.

#### Throws

Will throw an error if the commit ID is invalid or the request fails.

***

### getStreamState()

> **getStreamState**(`streamId`): `Promise`\<[`StreamState`](../../stream-client/type-aliases/StreamState.md)\>

Fetches the current stream state of a specific stream by its ID.

This method interacts with the Ceramic HTTP API to retrieve the state of a stream.
The stream ID can either be a multibase-encoded string or a `StreamID` object.

#### Parameters

• **streamId**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The unique identifier of the stream to fetch.
  - Can be a multibase-encoded string or a `StreamID` object.

#### Returns

`Promise`\<[`StreamState`](../../stream-client/type-aliases/StreamState.md)\>

A Promise that resolves to the `StreamState` object, representing the current state of the stream.

#### Throws

Will throw an error if the API request fails or returns an error response (if stream is not found).

#### Example

```typescript
const streamState = await client.getStreamState('kjzl6cwe1...');
console.log(streamState);
```

#### Inherited from

[`StreamClient`](../../stream-client/classes/StreamClient.md).[`getStreamState`](../../stream-client/classes/StreamClient.md#getstreamstate)

***

### postData()

> **postData**\<`T`\>(`params`): `Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

Posts a data event and returns its commit ID.

#### Type Parameters

• **T** *extends* [`UnknownContent`](../type-aliases/UnknownContent.md) = [`UnknownContent`](../type-aliases/UnknownContent.md)

#### Parameters

• **params**: [`PostDataParams`](../type-aliases/PostDataParams.md)\<`T`\>

Parameters for posting the data event.

#### Returns

`Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

A promise that resolves to the `CommitID` of the posted event.

#### Remarks

The data event updates the content of a stream and is associated with the
current state of the stream.

***

### postDeterministicInit()

> **postDeterministicInit**(`params`): `Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

Posts a deterministic initialization event and returns its commit ID.

#### Parameters

• **params**: [`PostDeterministicInitParams`](../type-aliases/PostDeterministicInitParams.md)

Parameters for posting the deterministic init event.

#### Returns

`Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

A promise that resolves to the `CommitID` of the posted event.

#### Remarks

This method ensures that the resulting stream ID is deterministic, derived
from the `uniqueValue` parameter. Commonly used for model instance documents
of type `set` and `single`.

***

### postSignedInit()

> **postSignedInit**\<`T`\>(`params`): `Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

Posts a signed initialization event and returns its commit ID.

#### Type Parameters

• **T** *extends* [`UnknownContent`](../type-aliases/UnknownContent.md) = [`UnknownContent`](../type-aliases/UnknownContent.md)

#### Parameters

• **params**: [`PostSignedInitParams`](../type-aliases/PostSignedInitParams.md)\<`T`\>

Parameters for posting the signed init event.

#### Returns

`Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

A promise that resolves to the `CommitID` of the posted event.

#### Remarks

This method results in a non-deterministic stream ID, typically used for
model instance documents of type `list`.

***

### streamStateToDocumentState()

> **streamStateToDocumentState**(`streamState`): `DocumentState`

Transforms a `StreamState` into a `DocumentState`.

#### Parameters

• **streamState**: [`StreamState`](../../stream-client/type-aliases/StreamState.md)

The stream state to transform.

#### Returns

`DocumentState`

The `DocumentState` derived from the stream state.

***

### updateDocument()

> **updateDocument**\<`T`\>(`params`): `Promise`\<`DocumentState`\>

Updates a document with new content and returns the updated document state.

#### Type Parameters

• **T** *extends* [`UnknownContent`](../type-aliases/UnknownContent.md) = [`UnknownContent`](../type-aliases/UnknownContent.md)

#### Parameters

• **params**: `UpdateDataParams`\<`T`\>

Parameters for updating the document.

#### Returns

`Promise`\<`DocumentState`\>

A promise that resolves to the updated `DocumentState`.

#### Remarks

This method posts the new content as a data event, updating the document.
It can optionally take the current document state to avoid re-fetching it.
