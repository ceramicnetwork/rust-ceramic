[**@ceramic-sdk/model-protocol v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-protocol](../README.md) / ModelRelationDefinition

# Variable: ModelRelationDefinition

> `const` **ModelRelationDefinition**: `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>

Identifies types of properties that are supported as relations by the indexing service.

Currently supported types of relation properties:
- 'account': references a DID property
- 'document': references a StreamID property with associated 'model' the related document must use
