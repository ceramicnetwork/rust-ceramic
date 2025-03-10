[**@ceramic-sdk/model-protocol v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/model-protocol](../README.md) / ModelAccountRelation

# Variable: ModelAccountRelation

> `const` **ModelAccountRelation**: `UnionCodec`\<[`ExactCodec`\<`TypeCodec`\<`object`\>\>, `ExactCodec`\<`TypeCodec`\<`object`\>\>]\>

Represents the relationship between an instance of this model and the controller account.
'list' means there can be many instances of this model for a single account. 'single' means
there can be only one instance of this model per account (if a new instance is created it
overrides the old one).
