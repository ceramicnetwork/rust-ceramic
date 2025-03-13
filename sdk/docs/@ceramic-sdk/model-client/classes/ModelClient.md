[**@ceramic-sdk/model-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-client](../README.md) / ModelClient

# Class: ModelClient

Represents a client for interacting with Ceramic models.

The `ModelClient` class extends the `StreamClient` class to provide additional
methods specific to working with Ceramic models, including fetching and creating
model definitions, retrieving initialization events, and decoding stream data.

## Extends

- [`StreamClient`](../../stream-client/classes/StreamClient.md)

## Constructors

### new ModelClient()

> **new ModelClient**(`params`): [`ModelClient`](ModelClient.md)

Creates a new instance of `StreamClient`.

#### Parameters

• **params**: [`StreamClientParams`](../../stream-client/type-aliases/StreamClientParams.md)

Configuration object containing parameters for initializing the StreamClient.

#### Returns

[`ModelClient`](ModelClient.md)

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

### createDefinition()

> **createDefinition**(`definition`, `signer`?): `Promise`\<[`StreamID`](../../identifiers/classes/StreamID.md)\>

Creates a model definition and returns the resulting stream ID.

#### Parameters

• **definition**: `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\> \| `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>

The model JSON definition to post.

• **signer?**: `DID`

(Optional) A `DID` instance for signing the model definition.

#### Returns

`Promise`\<[`StreamID`](../../identifiers/classes/StreamID.md)\>

A promise that resolves to the `StreamID` of the posted model.

#### Throws

Will throw an error if the definition is invalid or the signing process fails.

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

### getDocumentModel()

> **getDocumentModel**(`streamID`): `Promise`\<`string`\>

Retrieves the stringified model stream ID from a model instance document stream ID.

#### Parameters

• **streamID**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The document stream ID, either as a `StreamID` object or string.

#### Returns

`Promise`\<`string`\>

A promise that resolves to the stringified model stream ID.

#### Throws

Will throw an error if the stream ID or its state is invalid.

***

### getInitEvent()

> **getInitEvent**(`streamID`): `Promise`\<`MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>\>

Retrieves the signed initialization event of a model based on its stream ID.

#### Parameters

• **streamID**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The stream ID of the model, either as a `StreamID` object or string.

#### Returns

`Promise`\<`MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>\>

A promise that resolves to the `SignedEvent` for the model.

#### Throws

Will throw an error if the stream ID is invalid or the request fails.

***

### getModelDefinition()

> **getModelDefinition**(`streamID`): `Promise`\<`MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\> \| `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>\>

Retrieves a model's JSON definition based on the model's stream ID.

#### Parameters

• **streamID**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The stream ID of the model, either as a `StreamID` object or string.

#### Returns

`Promise`\<`MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\> \| `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>\>

A promise that resolves to the `ModelDefinition` for the specified model.

#### Throws

Will throw an error if the stream ID is invalid or the data cannot be decoded.

***

### getPayload()

> **getPayload**(`streamID`, `verifier`?): `Promise`\<`MapIn`\<`object`, `$TypeOf`\>\>

Retrieves the payload of the initialization event for a model based on its stream ID.

#### Parameters

• **streamID**: `string` \| [`StreamID`](../../identifiers/classes/StreamID.md)

The stream ID of the model, either as a `StreamID` object or string.

• **verifier?**: `DID`

(Optional) A `DID` instance for verifying the event payload.

#### Returns

`Promise`\<`MapIn`\<`object`, `$TypeOf`\>\>

A promise that resolves to the `ModelInitEventPayload`.

#### Throws

Will throw an error if the event or payload is invalid or verification fails.

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

### postDefinition()

> **postDefinition**(`params`): `Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

Posts a data event to a model stream and returns its commit ID.

#### Parameters

• **params**: [`PostDefinitionParams`](../type-aliases/PostDefinitionParams.md)

Parameters for posting the data event.

#### Returns

`Promise`\<[`CommitID`](../../identifiers/classes/CommitID.md)\>

A promise that resolves to the `CommitID` of the posted event.

#### Remarks

The data event updates the content of a stream and is associated with the
current state of the stream.

***

### streamStateToModelState()

> **streamStateToModelState**(`streamState`): [`ModelState`](../type-aliases/ModelState.md)

Transforms a `StreamState` into a `ModelState`.

#### Parameters

• **streamState**: [`StreamState`](../../stream-client/type-aliases/StreamState.md)

The stream state to transform.

#### Returns

[`ModelState`](../type-aliases/ModelState.md)

The `ModelState` derived from the stream state.

***

### updateDefinition()

> **updateDefinition**(`params`): `Promise`\<[`ModelState`](../type-aliases/ModelState.md)\>

Updates a model with a new definition and returns the updated model state.
Model's can only be updated in backwards compatible ways.

#### Parameters

• **params**: [`UpdateModelDefinitionParams`](../type-aliases/UpdateModelDefinitionParams.md)

Parameters for updating the document.

#### Returns

`Promise`\<[`ModelState`](../type-aliases/ModelState.md)\>

A promise that resolves to the updated `ModelState`.

#### Remarks

This method posts the new content as a data event, updating the document.
It can optionally take the current document state to avoid re-fetching it.
