[**@ceramic-sdk/model-protocol v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-protocol](../README.md) / ModelViewDefinitionV2

# Variable: ModelViewDefinitionV2

> `const` **ModelViewDefinitionV2**: `UnionCodec`\<[`UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>, `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>]\>

Identifies types of properties that are supported as view properties at DApps' runtime

A view-property is one that is not stored in related MIDs' content, but is derived from their other properties

Currently supported types of view properties:
- 'documentAccount': view properties of this type have the MID's controller DID as values
- 'documentVersion': view properties of this type have the MID's commit ID as values
- 'relationDocument': view properties of this type represent document relations identified by the given 'property' field
- 'relationFrom': view properties of this type represent inverse relations identified by the given 'model' and 'property' fields
- 'relationCountFrom': view properties of this type represent the number of inverse relations identified by the given 'model' and 'property' fields
- 'relationSetFrom': view properties of this type represent a single inverse relation identified by the given 'model' and 'property' fields for models using the SET account relation
