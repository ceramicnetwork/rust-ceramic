[**@ceramic-sdk/model-protocol v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-protocol](../README.md) / ModelDefinitionV2

# Variable: ModelDefinitionV2

> `const` **ModelDefinitionV2**: `SparseCodec`\<`object`\>

## Type declaration

### accountRelation

> **accountRelation**: `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\> = `ModelAccountRelationV2`

### description

> **description**: `OptionalCodec`\<`TrivialCodec`\<`string`\>\>

### immutableFields

> **immutableFields**: `OptionalCodec`\<`Codec`\<`string`[], `string`[], `unknown`\> & `object`\>

### implements

> **implements**: `Codec`\<`string`[], `string`[], `unknown`\> & `object`

### interface

> **interface**: `TrivialCodec`\<`boolean`\> = `boolean`

### name

> **name**: `TrivialCodec`\<`string`\> = `string`

### relations

> **relations**: `OptionalCodec`\<`NonEnumerableRecordCodec`\<`TrivialCodec`\<`string`\>, `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>\>\>

### schema

> **schema**: `Type`\<`Object`\<`any`\>, `Object`\<`any`\>, `unknown`\> = `ObjectSchema`

### version

> **version**: `LiteralCodec`\<`"2.0"`\>

### views

> **views**: `OptionalCodec`\<`NonEnumerableRecordCodec`\<`TrivialCodec`\<`string`\>, `UnionCodec`\<[`UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>, `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>]\>\>\>
