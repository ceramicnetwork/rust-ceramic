import {
  type SignedEvent,
  carFromString,
  carToString,
  eventFromString,
  eventToCAR,
} from '@ceramic-sdk/events'
import type { CAR } from 'cartonne'
import type { Codec, Decoder } from 'codeco'
import type { CID } from 'multiformats/cid'
import createAPIClient, { type HeadersOptions } from 'openapi-fetch'
import type { SimplifyDeep } from 'type-fest'

import type { components, paths } from './__generated__/api'

// Type definitions for Ceramic API and schemas
export type CeramicAPI = ReturnType<typeof createAPIClient<paths>>
export type Schemas = SimplifyDeep<components['schemas']>
export type Schema<Name extends keyof Schemas> = SimplifyDeep<Schemas[Name]>

// Parameters for initializing the Ceramic client
export type ClientParams = {
  /** Ceramic One server URL */
  url: string
  /** Custom fetch function to use for requests */
  fetch?: (request: Request) => ReturnType<typeof fetch>
  /** Additional HTTP headers to send with requests */
  headers?: HeadersOptions
}

// Parameters for fetching the events feed
export type EventsFeedParams = {
  /** Resume token for paginated feeds */
  resumeAt?: string
  /** Maximum number of events to return in response */
  limit?: number
}

/**
 * Represents an HTTP client for interacting with the Ceramic One server.
 *
 * This class provides methods for working with Ceramic events, including fetching, decoding, and posting events.
 * It also supports retrieving server metadata and managing stream interests.
 */
export class CeramicClient {
  /**
   * Internal OpenAPI client used for all HTTP requests to the Ceramic server.
   * This client provides a low-level interface for interacting with the server APIs.
   */
  #api: CeramicAPI

  /**
   * Creates a new instance of `CeramicClient`.
   *
   * @param params - Configuration options for initializing the client.
   * @param params.url - Base URL of the Ceramic One server.
   * @param params.fetch - (Optional) Custom fetch function to override default behavior.
   * @param params.headers - (Optional) Additional headers to include in HTTP requests.
   */
  constructor(params: ClientParams) {
    const { url, ...options } = params
    this.#api = createAPIClient<paths>({
      baseUrl: `${url}/ceramic`,
      ...options,
    })
  }

  /**
   * Retrieves the OpenAPI client instance.
   *
   * @returns The internal OpenAPI client used for making HTTP requests.
   */
  get api(): CeramicAPI {
    return this.#api
  }

  /**
   * Fetches a raw event response by its unique event CID.
   *
   * @param id - The unique identifier (CID) of the event to fetch.
   * @returns A Promise that resolves to the event schema.
   *
   * @throws Will throw an error if the request fails or the server returns an error.
   */
  async getEvent(id: string): Promise<Schema<'Event'>> {
    const { data, error } = await this.#api.GET('/events/{event_id}', {
      params: { path: { event_id: id } },
    })
    if (error != null) {
      throw new Error(`Failed to fetch event: ${error.message}`)
    }
    return data
  }

  /**
   * Fetches the string-encoded event data for a given event CID.
   *
   * @param id - The unique identifier (CID) of the event to fetch.
   * @returns A Promise that resolves to the event's string data.
   *
   * @throws Will throw an error if the event data is missing.
   */
  async getEventData(id: string): Promise<string> {
    const event = await this.getEvent(id)
    if (event.data == null) {
      throw new Error('Missing event data')
    }
    return event.data
  }

  /**
   * Fetches the CAR-encoded event for a given event CID.
   *
   * @param id - The unique identifier (CID) of the event.
   * @returns A Promise that resolves to a CAR object representing the event.
   */
  async getEventCAR(id: string): Promise<CAR> {
    const data = await this.getEventData(id)
    return carFromString(data)
  }

  /**
   * Decodes and retrieves a specific event type using the provided decoder.
   *
   * @param decoder - The decoder used to interpret the event data.
   * @param id - The unique identifier (CID) of the event.
   * @returns A Promise that resolves to the decoded event payload or a signed event.
   */
  async getEventType<Payload>(
    decoder: Decoder<unknown, Payload>,
    id: string,
  ): Promise<SignedEvent | Payload> {
    const data = await this.getEventData(id)
    return eventFromString(decoder, data)
  }

  /**
   * Retrieves the events feed based on provided parameters.
   *
   * @param params - Options for paginated feeds, including resume tokens and limits.
   * @returns A Promise that resolves to the events feed schema.
   */
  async getEventsFeed(
    params: EventsFeedParams = {},
  ): Promise<Schema<'EventFeed'>> {
    const { data, error } = await this.#api.GET('/feed/events', {
      query: params,
    })
    if (error != null) {
      throw new Error(`Failed to fetch events feed: ${error.message}`)
    }
    return data
  }

  /**
   * Retrieves the version information of the Ceramic One server.
   *
   * @returns A Promise that resolves to the server version schema.
   */
  async getVersion(): Promise<Schema<'Version'>> {
    const { data, error } = await this.#api.GET('/version')
    if (error != null) {
      throw new Error(`Failed to fetch server version: ${error.message}`)
    }
    return data
  }

  /**
   * Posts a string-encoded event to the server.
   *
   * @param data - The string-encoded event data to post.
   * @returns A Promise that resolves when the request completes.
   *
   * @throws Will throw an error if the request fails.
   */
  async postEvent(data: string): Promise<void> {
    const { error } = await this.#api.POST('/events', { body: { data } })
    if (error != null) {
      throw new Error(`Failed to post event: ${error.message}`)
    }
  }

  /**
   * Posts a CAR-encoded event to the server.
   *
   * @param car - The CAR object representing the event.
   * @returns A Promise that resolves to the CID of the posted event.
   */
  async postEventCAR(car: CAR): Promise<CID> {
    await this.postEvent(carToString(car))
    return car.roots[0]
  }

  /**
   * Encodes and posts an event using a specified codec.
   *
   * @param codec - The codec used to encode the event.
   * @param event - The event data to encode and post (must be compatible with the codec).
   * @returns A Promise that resolves to the CID of the posted event.
   */
  async postEventType(codec: Codec<unknown>, event: unknown): Promise<CID> {
    const car = eventToCAR(codec, event)
    return await this.postEventCAR(car)
  }

  /**
   * Registers interest in a model stream using its model stream ID.
   *
   * @param model - The stream ID of the model to register interest in.
   * @returns A Promise that resolves when the request completes.
   *
   * @throws Will throw an error if the request fails.
   */
  async registerInterestModel(model: string): Promise<void> {
    const { error } = await this.#api.POST('/interests', {
      body: { sep: 'model', sepValue: model },
    })
    if (error != null) {
      throw new Error(`Failed to register interest in model: ${error.message}`)
    }
  }
}

/**
 * Internal utility function to retrieve or initialize a Ceramic client.
 *
 * @param ceramic - Either a Ceramic client instance or a Ceramic server URL.
 * @returns A Ceramic client instance.
 */
export function getCeramicClient(
  ceramic: CeramicClient | string,
): CeramicClient {
  return typeof ceramic === 'string'
    ? new CeramicClient({ url: ceramic })
    : ceramic
}
