[**@ceramic-sdk/identifiers v0.2.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/identifiers](../README.md) / StreamID

# Class: StreamID

Stream identifier, no commit information included.

Contains stream type and CID of genesis commit.

Encoded as `<multibase-prefix><multicodec-streamid><type><genesis-cid-bytes>`.

String representation is base36-encoding of the bytes above.

## Constructors

### new StreamID()

> **new StreamID**(`type`, `cid`): [`StreamID`](StreamID.md)

Create a new StreamID.

#### Parameters

• **type**: [`StreamType`](../type-aliases/StreamType.md)

the stream type

• **cid**: `string` \| `CID`\<`unknown`, `number`, `number`, `Version`\>

#### Returns

[`StreamID`](StreamID.md)

#### Example

```typescript
new StreamID('MID', 'bagcqcerakszw2vsovxznyp5gfnpdj4cqm2xiv76yd24wkjewhhykovorwo6a');
new StreamID('MID', cid);
new StreamID(3, cid);
```

## Properties

### \_tag

> `protected` `readonly` **\_tag**: `symbol` = `TAG`

***

### fromBytes()

> `static` **fromBytes**: (`bytes`) => [`StreamID`](StreamID.md)

Parse StreamID from bytes representation.

#### Parameters

• **bytes**: `Uint8Array`

bytes representation of StreamID.

#### Returns

[`StreamID`](StreamID.md)

#### Throws

error on invalid input

#### See

StreamID#bytes

***

### fromString()

> `static` **fromString**: (`input`) => [`StreamID`](StreamID.md)

Parse StreamID from string representation.

#### Parameters

• **input**: `string`

string representation of StreamID, be it base36-encoded string or URL.

#### Returns

[`StreamID`](StreamID.md)

#### See

 - StreamID#toString
 - StreamID#toUrl

## Accessors

### baseID

> `get` **baseID**(): [`StreamID`](StreamID.md)

Copy of self. Exists to maintain compatibility with CommitID.

#### Returns

[`StreamID`](StreamID.md)

#### Defined in

***

### bytes

> `get` **bytes**(): `Uint8Array`

Bytes representation of StreamID.

#### Returns

`Uint8Array`

#### Defined in

***

### cid

> `get` **cid**(): `CID`\<`unknown`, `number`, `number`, `Version`\>

Genesis commits CID

#### Returns

`CID`\<`unknown`, `number`, `number`, `Version`\>

#### Defined in

***

### type

> `get` **type**(): [`StreamTypeCode`](../type-aliases/StreamTypeCode.md)

Stream type code

#### Returns

[`StreamTypeCode`](../type-aliases/StreamTypeCode.md)

#### Defined in

***

### typeName

> `get` **typeName**(): `"tile"` \| `"caip10-link"` \| `"model"` \| `"MID"` \| `"UNLOADABLE"`

Stream type name

#### Returns

`"tile"` \| `"caip10-link"` \| `"model"` \| `"MID"` \| `"UNLOADABLE"`

#### Defined in

## Methods

### equals()

> **equals**(`other`): `boolean`

Compare equality with another StreamID.

#### Parameters

• **other**: [`StreamID`](StreamID.md)

#### Returns

`boolean`

***

### toString()

> **toString**(): `string`

Encode the StreamID into a string.

#### Returns

`string`

***

### toUrl()

> **toUrl**(): `string`

Encode the StreamID into a base36 url.

#### Returns

`string`

***

### fromPayload()

> `static` **fromPayload**(`type`, `value`): [`StreamID`](StreamID.md)

Create a streamId from an init event payload.

#### Parameters

• **type**: [`StreamType`](../type-aliases/StreamType.md)

the stream type

• **value**: `unknown`

the init event payload

#### Returns

[`StreamID`](StreamID.md)

#### Example

```typescript
const streamId = StreamID.fromPayload('MID', {
  header: {
    controllers: ['did:3:kjz...'],
    model: '...',
  }
});
```

***

### isInstance()

> `static` **isInstance**(`instance`): `instance is StreamID`

#### Parameters

• **instance**: `unknown`

#### Returns

`instance is StreamID`
