[**@ceramic-sdk/events v0.2.1**](../README.md) • **Docs**

***

[Ceramic SDK](../../../README.md) / [@ceramic-sdk/events](../README.md) / PartialInitEventHeader

# Type Alias: PartialInitEventHeader

> **PartialInitEventHeader**: `Omit`\<[`InitEventHeader`](InitEventHeader.md), `"controllers"`\> & `object`

A partial version of the initialization event header, with optional controllers.

## Type declaration

### controllers?

> `optional` **controllers**: [`DIDString`]

## Remarks

This type is used to allow the creation of initialization events without requiring
the `controllers` field, as it will be injected automatically during the signing process.
