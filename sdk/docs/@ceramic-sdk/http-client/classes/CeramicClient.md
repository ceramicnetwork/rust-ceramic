[**@ceramic-sdk/http-client v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/http-client](../README.md) / CeramicClient

# Class: CeramicClient

Represents an HTTP client for interacting with the Ceramic One server.

This class provides methods for working with Ceramic events, including fetching, decoding, and posting events.
It also supports retrieving server metadata and managing stream interests.

## Constructors

### new CeramicClient()

> **new CeramicClient**(`params`): [`CeramicClient`](CeramicClient.md)

Creates a new instance of `CeramicClient`.

#### Parameters

• **params**: [`ClientParams`](../type-aliases/ClientParams.md)

Configuration options for initializing the client.

#### Returns

[`CeramicClient`](CeramicClient.md)

## Accessors

### api

> `get` **api**(): `Client`\<`paths`, \`$\{string\}/$\{string\}\`\>

Retrieves the OpenAPI client instance.

#### Returns

`Client`\<`paths`, \`$\{string\}/$\{string\}\`\>

The internal OpenAPI client used for making HTTP requests.

#### Defined in

## Methods

### getEvent()

> **getEvent**(`id`): `Promise`\<`object`\>

Fetches a raw event response by its unique event CID.

#### Parameters

• **id**: `string`

The unique identifier (CID) of the event to fetch.

#### Returns

`Promise`\<`object`\>

A Promise that resolves to the event schema.

##### data?

> `optional` **data**: `string`

Multibase encoding of event data.

##### id

> **id**: `string`

Multibase encoding of event root CID bytes.

#### Throws

Will throw an error if the request fails or the server returns an error.

***

### getEventCAR()

> **getEventCAR**(`id`): `Promise`\<`CAR`\>

Fetches the CAR-encoded event for a given event CID.

#### Parameters

• **id**: `string`

The unique identifier (CID) of the event.

#### Returns

`Promise`\<`CAR`\>

A Promise that resolves to a CAR object representing the event.

***

### getEventData()

> **getEventData**(`id`): `Promise`\<`string`\>

Fetches the string-encoded event data for a given event CID.

#### Parameters

• **id**: `string`

The unique identifier (CID) of the event to fetch.

#### Returns

`Promise`\<`string`\>

A Promise that resolves to the event's string data.

#### Throws

Will throw an error if the event data is missing.

***

### getEventsFeed()

> **getEventsFeed**(`params`): `Promise`\<`object`\>

Retrieves the events feed based on provided parameters.

#### Parameters

• **params**: [`EventsFeedParams`](../type-aliases/EventsFeedParams.md) = `{}`

Options for paginated feeds, including resume tokens and limits.

#### Returns

`Promise`\<`object`\>

A Promise that resolves to the events feed schema.

##### events

> **events**: `object`[]

An array of events. For now, the event data payload is empty.

##### resumeToken

> **resumeToken**: `string`

The token/highwater mark to used as resumeAt on a future request

***

### getEventType()

> **getEventType**\<`Payload`\>(`decoder`, `id`): `Promise`\<`Payload` \| `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>\>

Decodes and retrieves a specific event type using the provided decoder.

#### Type Parameters

• **Payload**

#### Parameters

• **decoder**: `Decoder`\<`unknown`, `Payload`\>

The decoder used to interpret the event data.

• **id**: `string`

The unique identifier (CID) of the event.

#### Returns

`Promise`\<`Payload` \| `MapIn`\<`RequiredProps`\<`object`\>, `$TypeOf`\> & `MapIn`\<`OptionalProps`\<`object`\>, `$TypeOf`\>\>

A Promise that resolves to the decoded event payload or a signed event.

***

### getVersion()

> **getVersion**(): `Promise`\<`object`\>

Retrieves the version information of the Ceramic One server.

#### Returns

`Promise`\<`object`\>

A Promise that resolves to the server version schema.

##### version?

> `optional` **version**: `string`

Version of the Ceramic node

***

### postEvent()

> **postEvent**(`data`): `Promise`\<`void`\>

Posts a string-encoded event to the server.

#### Parameters

• **data**: `string`

The string-encoded event data to post.

#### Returns

`Promise`\<`void`\>

A Promise that resolves when the request completes.

#### Throws

Will throw an error if the request fails.

***

### postEventCAR()

> **postEventCAR**(`car`): `Promise`\<`CID`\<`unknown`, `number`, `number`, `Version`\>\>

Posts a CAR-encoded event to the server.

#### Parameters

• **car**: `CAR`

The CAR object representing the event.

#### Returns

`Promise`\<`CID`\<`unknown`, `number`, `number`, `Version`\>\>

A Promise that resolves to the CID of the posted event.

***

### postEventType()

> **postEventType**(`codec`, `event`): `Promise`\<`CID`\<`unknown`, `number`, `number`, `Version`\>\>

Encodes and posts an event using a specified codec.

#### Parameters

• **codec**: `Codec`\<`unknown`, `unknown`, `unknown`\>

The codec used to encode the event.

• **event**: `unknown`

The event data to encode and post (must be compatible with the codec).

#### Returns

`Promise`\<`CID`\<`unknown`, `number`, `number`, `Version`\>\>

A Promise that resolves to the CID of the posted event.

***

### registerInterestModel()

> **registerInterestModel**(`model`): `Promise`\<`void`\>

Registers interest in a model stream using its model stream ID.

#### Parameters

• **model**: `string`

The stream ID of the model to register interest in.

#### Returns

`Promise`\<`void`\>

A Promise that resolves when the request completes.

#### Throws

Will throw an error if the request fails.
