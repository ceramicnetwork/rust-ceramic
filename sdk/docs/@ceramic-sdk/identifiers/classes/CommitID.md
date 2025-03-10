[**@ceramic-sdk/identifiers v0.2.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/identifiers](../README.md) / CommitID

# Class: CommitID

Commit identifier, includes type, init CID, commit CID.

Encoded as `<multibase-prefix><multicodec-streamid><type><init-cid-bytes><commit-cid-bytes>`.

String representation is base36-encoding of the bytes above.

## Constructors

### new CommitID()

> **new CommitID**(`type`, `cid`, `commit`): [`CommitID`](CommitID.md)

Create a new StreamID.

#### Parameters

• **type**: [`StreamType`](../type-aliases/StreamType.md)

• **cid**: `string` \| `CID`\<`unknown`, `number`, `number`, `Version`\>

• **commit**: `null` \| `string` \| `number` \| `CID`\<`unknown`, `number`, `number`, `Version`\> = `null`

CID. Pass '0', 0, or omit the value as shorthand for the init commit.

#### Returns

[`CommitID`](CommitID.md)

#### Example

```ts
new StreamID(<type>, <CID>|<cidStr>)
new StreamID(<type>, <CID>|<cidStr>, <CommitCID>|<CommitCidStr>)
```

## Properties

### \_tag

> `protected` `readonly` **\_tag**: `symbol` = `TAG`

***

### fromBytes()

> `static` **fromBytes**: (`bytes`) => [`CommitID`](CommitID.md)

Parse CommitID from bytes representation.

#### Parameters

• **bytes**: `Uint8Array`

bytes representation of CommitID.

#### Returns

[`CommitID`](CommitID.md)

#### Throws

error on invalid input

#### See

CommitID#bytes

***

### fromString()

> `static` **fromString**: (`input`) => [`CommitID`](CommitID.md)

Parse CommitID from string representation.

#### Parameters

• **input**: `string`

string representation of CommitID, be it base36-encoded string or URL.

#### Returns

[`CommitID`](CommitID.md)

#### See

 - CommitID#toString
 - CommitID#toUrl

## Accessors

### baseID

> `get` **baseID**(): [`StreamID`](StreamID.md)

Construct StreamID, no commit information included

#### Returns

[`StreamID`](StreamID.md)

#### Defined in

***

### bytes

> `get` **bytes**(): `Uint8Array`

Bytes representation

#### Returns

`Uint8Array`

#### Defined in

***

### cid

> `get` **cid**(): `CID`\<`unknown`, `number`, `number`, `Version`\>

Genesis CID

#### Returns

`CID`\<`unknown`, `number`, `number`, `Version`\>

#### Defined in

***

### commit

> `get` **commit**(): `CID`\<`unknown`, `number`, `number`, `Version`\>

Commit CID

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

Compare equality with another CommitID.

#### Parameters

• **other**: [`CommitID`](CommitID.md)

#### Returns

`boolean`

***

### toString()

> **toString**(): `string`

Encode the CommitID into a string.

#### Returns

`string`

***

### toUrl()

> **toUrl**(): `string`

Encode the StreamID into a base36 url

#### Returns

`string`

***

### fromStream()

> `static` **fromStream**(`stream`, `commit`): [`CommitID`](CommitID.md)

Construct new CommitID for a given stream and commit

#### Parameters

• **stream**: [`StreamID`](StreamID.md)

• **commit**: `null` \| `string` \| `number` \| `CID`\<`unknown`, `number`, `number`, `Version`\> = `null`

#### Returns

[`CommitID`](CommitID.md)

***

### isInstance()

> `static` **isInstance**(`instance`): `instance is CommitID`

#### Parameters

• **instance**: `unknown`

#### Returns

`instance is CommitID`
