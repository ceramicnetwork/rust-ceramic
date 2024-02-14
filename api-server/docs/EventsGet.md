# EventsGet

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**events** | [**Vec<models::Event>**](Event.md) | An array of events | 
**resume_offset** | **i32** | An integer specifying where to resume the request. Only works correctly for the same input parameters. | 
**is_complete** | **bool** | A boolean specifying if there are more events to be fetched. Repeat with the resumeOffset to get next set. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


