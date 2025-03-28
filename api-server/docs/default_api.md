# default_api

All URIs are relative to */ceramic*

Method | HTTP request | Description
------------- | ------------- | -------------
****](default_api.md#) | **GET** /config/network | Get info about the Ceramic network the node is connected to
****](default_api.md#) | **OPTIONS** /config/network | cors
****](default_api.md#) | **GET** /debug/heap | Get the heap statistics of the Ceramic node
****](default_api.md#) | **OPTIONS** /debug/heap | cors
****](default_api.md#) | **GET** /events/{event_id} | Get event data
****](default_api.md#) | **OPTIONS** /events/{event_id} | cors
****](default_api.md#) | **OPTIONS** /events | cors
****](default_api.md#) | **POST** /events | Creates a new event
****](default_api.md#) | **GET** /experimental/events/{sep}/{sepValue} | Get events matching the interest stored on the node
****](default_api.md#) | **OPTIONS** /experimental/events/{sep}/{sepValue} | cors
****](default_api.md#) | **GET** /experimental/interests | Get the interests stored on the node
****](default_api.md#) | **OPTIONS** /experimental/interests | cors
****](default_api.md#) | **GET** /feed/events | Get all new event keys since resume token
****](default_api.md#) | **OPTIONS** /feed/events | cors
****](default_api.md#) | **GET** /feed/resumeToken | Get the current (maximum) highwater mark/continuation token of the feed. Allows starting `feed/events` from 'now'.
****](default_api.md#) | **OPTIONS** /feed/resumeToken | cors
****](default_api.md#) | **OPTIONS** /interests | cors
****](default_api.md#) | **POST** /interests | Register interest for a sort key
****](default_api.md#) | **OPTIONS** /interests/{sort_key}/{sort_value} | cors
****](default_api.md#) | **POST** /interests/{sort_key}/{sort_value} | Register interest for a sort key
****](default_api.md#) | **GET** /liveness | Test the liveness of the Ceramic node
****](default_api.md#) | **OPTIONS** /liveness | cors
****](default_api.md#) | **GET** /peers | Get list of connected peers
****](default_api.md#) | **OPTIONS** /peers | cors
****](default_api.md#) | **POST** /peers | Connect to a peer
****](default_api.md#) | **GET** /streams/{stream_id} | Get stream state
****](default_api.md#) | **OPTIONS** /streams/{stream_id} | cors
****](default_api.md#) | **GET** /version | Get the version of the Ceramic node
****](default_api.md#) | **OPTIONS** /version | cors
****](default_api.md#) | **POST** /version | Get the version of the Ceramic node


# ****
> models::NetworkInfo ()
Get info about the Ceramic network the node is connected to

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::NetworkInfo**](NetworkInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> ()
cors

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
> swagger::ByteArray ()
Get the heap statistics of the Ceramic node

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**swagger::ByteArray**](file.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> ()
cors

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
> models::Event (event_id)
Get event data

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **event_id** | **String**| CID of the root block of the event, used to identify of the event | 

### Return type

[**models::Event**](Event.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (event_id)
cors

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **event_id** | **String**| Name of the field in the Events header that holds the separator value e.g. 'model' | 

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
cors

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
> (event_data)
Creates a new event

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **event_data** | [**EventData**](EventData.md)| Event to add to the node | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::EventsGet (sep, sep_value, optional)
Get events matching the interest stored on the node

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **sep** | **String**| Name of the field in the Events header that holds the separator value e.g. 'model' | 
  **sep_value** | **String**| The value of the field in the Events header indicated by the separator key e.g. multibase encoded model ID | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sep** | **String**| Name of the field in the Events header that holds the separator value e.g. 'model' | 
 **sep_value** | **String**| The value of the field in the Events header indicated by the separator key e.g. multibase encoded model ID | 
 **controller** | **String**| the controller to filter (DID string) | 
 **stream_id** | **String**| the stream to filter (multibase encoded stream ID) | 
 **offset** | **i32**| token that designates the point to resume from, that is find keys added after this point | 
 **limit** | **i32**| the maximum number of events to return, default is 10000. | 

### Return type

[**models::EventsGet**](EventsGet.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (sep, sep_value)
cors

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **sep** | **String**| Name of the field in the Events header that holds the separator value e.g. 'model' | 
  **sep_value** | **String**| The value of the field in the Events header indicated by the separator key e.g. multibase encoded model ID | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::InterestsGet (optional)
Get the interests stored on the node

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **peer_id** | **String**| Only return interests from the specified peer ID. | 

### Return type

[**models::InterestsGet**](InterestsGet.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (optional)
cors

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **peer_id** | **String**| Only return interests from the specified peer ID. | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
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
 **limit** | **i32**| The maximum number of events to return, default is 100. The max with data is 10000. | 
 **include_data** | **String**| Whether to include the event data (carfile) in the response. In the future, only the payload or other options may be supported:   * `none` - Empty, only the event ID is returned   * `full` - The entire event carfile (including the envelope and payload)  | 

### Return type

[**models::EventFeed**](EventFeed.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> ()
cors

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
> models::FeedResumeTokenGet200Response ()
Get the current (maximum) highwater mark/continuation token of the feed. Allows starting `feed/events` from 'now'.

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::FeedResumeTokenGet200Response**](_feed_resumeToken_get_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> ()
cors

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
> ()
cors

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
> (interest)
Register interest for a sort key

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **interest** | [**Interest**](Interest.md)| Interest to register on the node | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (sort_key, sort_value)
cors

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **sort_key** | **String**| Name of the field in the Events header that holds the separator value e.g. 'model' | 
  **sort_value** | **String**| The value of the field in the Events header indicated by the separator key e.g. multibase encoded model ID | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

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
 - **Accept**: application/json

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
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> ()
cors

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
> models::Peers ()
Get list of connected peers

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::Peers**](Peers.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (addresses)
cors

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **addresses** | [**String**](String.md)| Multiaddress of peer to connect to, at least one address must contain the peer id. | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (addresses)
Connect to a peer

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **addresses** | [**String**](String.md)| Multiaddress of peer to connect to, at least one address must contain the peer id. | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::StreamState (stream_id)
Get stream state

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **stream_id** | **String**| Multibase encoded stream ID | 

### Return type

[**models::StreamState**](StreamState.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (stream_id)
cors

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **stream_id** | **String**| Multibase encoded stream ID | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

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

# ****
> ()
cors

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

