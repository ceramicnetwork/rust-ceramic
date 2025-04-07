[**@ceramic-sdk/model-protocol v0.7.0**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-protocol](../README.md) / ModelAccountRelationV2

# Variable: ModelAccountRelationV2

> `const` **ModelAccountRelationV2**: `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>

Represents the relationship between an instance of this model and the controller account:
- 'list' means there can be many instances of this model for a single account
- 'single' means there can be only one instance of this model per account (if a new instance is created it
overrides the old one)
- 'none' means there can be no instance associated to an account (for interfaces notably)
- 'set' means there can be only one instance of this model per account and value of the specified content 'fields'
