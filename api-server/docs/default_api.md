# default_api

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
****](default_api.md#) | **POST** /ceramic/events | Creates a new event
****](default_api.md#) | **GET** /ceramic/subscribe/{sortkey}/{sortvalue} | Get events for a stream


# ****
> (event)
Creates a new event

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **event** | [**Event**](Event.md)| Event to add to the node | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> Vec<models::Event> (sortkey, sortvalue, optional)
Get events for a stream

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **sortkey** | **String**| name of the sort-key | 
  **sortvalue** | **String**| value associated with the sort key | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sortkey** | **String**| name of the sort-key | 
 **sortvalue** | **String**| value associated with the sort key | 
 **controller** | **String**| the controller to subscribe to. | 
 **stream_id** | **String**| the stream to subscribe to. | 
 **offset** | **f64**| the number of events to skip in the given range, default is 0. | 
 **limit** | **f64**| the maximum number of events to return, default is no limit. | 

### Return type

[**Vec<models::Event>**](Event.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

