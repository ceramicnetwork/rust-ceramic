# default_api

All URIs are relative to */api/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
****](default_api.md#) | **POST** /block/get | Get a single IPFS block
****](default_api.md#) | **POST** /block/put | Put a single IPFS block
****](default_api.md#) | **POST** /block/stat | Report statistics about a block
****](default_api.md#) | **POST** /dag/get | Get an IPLD node from IPFS
****](default_api.md#) | **POST** /dag/import | Import a CAR file of IPLD nodes into IPFS
****](default_api.md#) | **POST** /dag/put | Put an IPLD node into IPFS
****](default_api.md#) | **POST** /dag/resolve | Resolve an IPFS path to a DAG node
****](default_api.md#) | **POST** /id | Report identifying information about a node
****](default_api.md#) | **POST** /pin/add | Add a block to the pin store
****](default_api.md#) | **POST** /pin/rm | Remove a block from the pin store
****](default_api.md#) | **POST** /pubsub/ls | List topic with active subscriptions
****](default_api.md#) | **POST** /pubsub/pub | Publish a message to a topic
****](default_api.md#) | **POST** /pubsub/sub | Subscribe to a topic, blocks until a message is received
****](default_api.md#) | **POST** /swarm/connect | Connect to peers
****](default_api.md#) | **POST** /swarm/peers | Report connected peers
****](default_api.md#) | **POST** /version | Report server version


# ****
> swagger::ByteArray (arg, optional)
Get a single IPFS block

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| CID of block | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **arg** | **String**| CID of block | 
 **timeout** | **String**| Max duration (as Go duration string) to wait to find the block | 

### Return type

[**swagger::ByteArray**](file.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::BlockPutPost200Response (file, optional)
Put a single IPFS block

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **file** | **swagger::ByteArray**|  | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **file** | **swagger::ByteArray**|  | 
 **cid_codec** | [****](.md)| Codec of the block data | 
 **mhtype** | [****](.md)| Multihash type | 
 **pin** | [****](.md)| Whether to recursively pin the block | 

### Return type

[**models::BlockPutPost200Response**](_block_put_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::BlockPutPost200Response (arg)
Report statistics about a block

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| CID of block | 

### Return type

[**models::BlockPutPost200Response**](_block_put_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> swagger::ByteArray (arg, optional)
Get an IPLD node from IPFS

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| IPFS path to DAG node | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **arg** | **String**| IPFS path to DAG node | 
 **output_codec** | [****](.md)| Output encoding of the data | 

### Return type

[**swagger::ByteArray**](file.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::DagImportPost200Response (file)
Import a CAR file of IPLD nodes into IPFS

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **file** | **swagger::ByteArray**|  | 

### Return type

[**models::DagImportPost200Response**](_dag_import_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::DagPutPost200Response (file, optional)
Put an IPLD node into IPFS

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **file** | **swagger::ByteArray**|  | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **file** | **swagger::ByteArray**|  | 
 **store_codec** | [****](.md)| IPFS path to DAG node | 
 **input_codec** | [****](.md)| Output encoding of the data | 

### Return type

[**models::DagPutPost200Response**](_dag_put_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::DagResolvePost200Response (arg)
Resolve an IPFS path to a DAG node

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| IPFS path to DAG node | 

### Return type

[**models::DagResolvePost200Response**](_dag_resolve_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::IdPost200Response (optional)
Report identifying information about a node

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **arg** | **String**| Peer ID of peer | 

### Return type

[**models::IdPost200Response**](_id_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::PinAddPost200Response (arg, optional)
Add a block to the pin store

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| CID of block | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **arg** | **String**| CID of block | 
 **recursive** | [****](.md)| When true recursively pin all blocks | 
 **progress** | [****](.md)| Report pin progress | 

### Return type

[**models::PinAddPost200Response**](_pin_add_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::PinAddPost200Response (arg)
Remove a block from the pin store

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| CID of block | 

### Return type

[**models::PinAddPost200Response**](_pin_add_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::PubsubLsPost200Response ()
List topic with active subscriptions

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::PubsubLsPost200Response**](_pubsub_ls_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> (arg, file)
Publish a message to a topic

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| Multibase encoded topic name | 
  **file** | **swagger::ByteArray**|  | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> swagger::ByteArray (arg)
Subscribe to a topic, blocks until a message is received

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | **String**| Multibase encoded topic name | 

### Return type

[**swagger::ByteArray**](file.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::PubsubLsPost200Response (arg)
Connect to peers

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
  **arg** | [**String**](String.md)| Addresses of peers | 

### Return type

[**models::PubsubLsPost200Response**](_pubsub_ls_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::SwarmPeersPost200Response ()
Report connected peers

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::SwarmPeersPost200Response**](_swarm_peers_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# ****
> models::VersionPost200Response ()
Report server version

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::VersionPost200Response**](_version_post_200_response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

