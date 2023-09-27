# default_api

All URIs are relative to */ceramic*

Method | HTTP request | Description
------------- | ------------- | -------------
****](default_api.md#) | **POST** /events | Creates a new event
****](default_api.md#) | **GET** /liveness | Test the liveness of the Ceramic node
****](default_api.md#) | **POST** /recon | Sends a Recon message
****](default_api.md#) | **GET** /subscribe/{sort_key}/{sort_value} | Get events for a stream
****](default_api.md#) | **POST** /version | Get the version of the Ceramic node


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
> ()
Test the liveness of the Ceramic node

### Required Parameters
This endpoint does not need any parameter.

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> swagger::ByteArray (ring, body)
Sends a Recon message

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **ring** | [****](.md)| Recon ring | 
  **body** | **swagger::ByteArray**| Recon message to send | 

### Return type

[**swagger::ByteArray**](file.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/cbor-seq
 - **Accept**: application/cbor-seq

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> Vec<models::Event> (sort_key, sort_value, optional)
Get events for a stream

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **sort_key** | **String**| name of the sort_key | 
  **sort_value** | **String**| value associated with the sort key | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sort_key** | **String**| name of the sort_key | 
 **sort_value** | **String**| value associated with the sort key | 
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

# ****
> models::Version ()
Get the version of the Ceramic node

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::Version**](Version.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

