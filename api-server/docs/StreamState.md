# StreamState

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** | Multibase encoding of the stream id | 
**event_cid** | **String** | CID of the event that produced this state | 
**controller** | **String** | Controller of the stream | 
**dimensions** | [***serde_json::Value**](.md) | Dimensions of the stream, each value is multibase encoded. | 
**data** | **String** | Multibase encoding of the data of the stream. Content is stream type specific. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


