[**@ceramic-sdk/model-protocol v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-protocol](../README.md) / ModelRelationDefinitionV2

# Variable: ModelRelationDefinitionV2

> `const` **ModelRelationDefinitionV2**: `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>

Identifies types of properties that are supported as relations by the indexing service.

Currently supported types of relation properties:
- 'account': references a DID property
- 'document': references a StreamID property with associated 'model' the related document must use if provided
