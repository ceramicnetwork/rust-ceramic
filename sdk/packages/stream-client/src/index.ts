import { type CeramicClient, getCeramicClient } from '@ceramic-sdk/http-client'
import type { StreamID } from '@ceramic-sdk/identifiers'
import type { DID } from 'dids'

export type StreamState = {
  /** Multibase encoding of the stream id */
  id: string
  /** CID of the event that produced this state */
  event_cid: string
  /** Controller of the stream */
  controller: string
  /** Dimensions of the stream, each value is multibase encoded */
  dimensions: Record<string, string>
  /** Multibase encoding of the data of the stream. Content is stream type specific */
  data: string
}

export type StreamClientParams = {
  /** Ceramic HTTP client instance or Ceramic One server URL */
  ceramic: CeramicClient | string
  /** DID to use by default in method calls */
  did?: DID
}

/**
 * Represents a Stream Client.
 *
 * This class provides basic utility methods to interact with streams, including
 * accessing the Ceramic client and obtaining stream states. It also manages a
 * Decentralized Identifier (DID) that can be used for authentication or signing.
 *
 * This class is intended to be extended by other classes for more specific functionalities.
 */
export class StreamClient {
  /**
   * Instance of the Ceramic HTTP client, used to communicate with the Ceramic node.
   * It facilitates API interactions such as fetching stream states or publishing updates.
   */
  #ceramic: CeramicClient

  /**
   * Optional Decentralized Identifier (DID) attached to the instance.
   * The DID is typically used for authentication or signing event payloads.
   */
  #did?: DID

  /**
   * Creates a new instance of `StreamClient`.
   *
   * @param params - Configuration object containing parameters for initializing the StreamClient.
   * @param params.ceramic - URL or instance of the Ceramic client to be used.
   * @param params.did - Optional DID to be attached to the client for authentication purposes.
   */
  constructor(params: StreamClientParams) {
    // Initialize the Ceramic client from the provided parameters
    this.#ceramic = getCeramicClient(params.ceramic)

    // Attach the provided DID to the instance (if any)
    this.#did = params.did
  }

  /**
   * Retrieves the Ceramic HTTP client instance used to interact with the Ceramic server.
   *
   * This client is essential for executing API requests to fetch stream data.
   *
   * @returns The `CeramicClient` instance associated with this StreamClient.
   */
  get ceramic(): CeramicClient {
    return this.#ceramic
  }

  /**
   * Fetches the current stream state of a specific stream by its ID.
   *
   * This method interacts with the Ceramic HTTP API to retrieve the state of a stream.
   * The stream ID can either be a multibase-encoded string or a `StreamID` object.
   *
   * @param streamId - The unique identifier of the stream to fetch.
   *   - Can be a multibase-encoded string or a `StreamID` object.
   * @returns A Promise that resolves to the `StreamState` object, representing the current state of the stream.
   *
   * @throws Will throw an error if the API request fails or returns an error response (if stream is not found).
   *
   * @example
   * ```typescript
   * const streamState = await client.getStreamState('kjzl6cwe1...');
   * console.log(streamState);
   * ```
   */
  async getStreamState(streamId: StreamID | string): Promise<StreamState> {
    // Prepare the API request to fetch the stream state
    const { data, error } = await this.#ceramic.api.GET(
      '/streams/{stream_id}',
      {
        params: {
          path: {
            stream_id:
              typeof streamId === 'string' ? streamId : streamId.toString(),
          },
        },
      },
    )

    // Handle errors returned from the API
    if (error != null) {
      throw new Error(`Failed to fetch stream state: ${error.message}`)
    }

    // Return the retrieved stream state
    return data
  }

  /**
   * Retrieves a Decentralized Identifier (DID).
   *
   * This method provides access to a DID, which is required for authentication or event signing operations.
   * The caller can optionally provide a DID; if none is provided, the instance's DID is used instead.
   *
   * @param provided - An optional DID object to use. If not supplied, the instance's DID is returned.
   * @returns The DID object, either provided or attached to the instance.
   *
   * @throws Will throw an error if neither a provided DID nor an instance DID is available.
   *
   * @example
   * ```typescript
   * const did = client.getDID();
   * console.log(did.id); // Outputs the DID identifier
   * ```
   */
  getDID(provided?: DID): DID {
    if (provided != null) {
      return provided
    }
    if (this.#did != null) {
      return this.#did
    }
    throw new Error(
      'Missing DID: Neither a provided DID nor an instance DID is available.',
    )
  }
}
