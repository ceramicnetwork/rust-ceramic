# default_api

All URIs are relative to */ceramic*

Method | HTTP request | Description
------------- | ------------- | -------------
****](default_api.md#) | **POST** /events | Creates a new event
****](default_api.md#) | **GET** /feed/events | Get all new event keys since resume token
****](default_api.md#) | **POST** /interests/{sort_key}/{sort_value} | Register interest for a sort key
****](default_api.md#) | **GET** /liveness | Test the liveness of the Ceramic node
****](default_api.md#) | **GET** /subscribe/{sort_key}/{sort_value} | Get events for a stream
****](default_api.md#) | **POST** /version | Get the version of the Ceramic node


# ****
> (event_deprecated)
Creates a new event

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **event_deprecated** | [**EventDeprecated**](EventDeprecated.md)| Event to add to the node | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::EventFeed (optional)
Get all new event keys since resume token

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **resume_at** | **String**| token that designates the point to resume from, that is find keys added after this point | 
 **limit** | **i32**| the maximum number of events to return, default is 10000. | 

### Return type

[**models::EventFeed**](EventFeed.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (sort_key, sort_value, optional)
Register interest for a sort key

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
 **controller** | **String**| the controller to register interest for | 
 **stream_id** | **String**| the stream to register interest for | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
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
> Vec<models::EventDeprecated> (sort_key, sort_value, optional)
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

[**Vec<models::EventDeprecated>**](EventDeprecated.md)

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

