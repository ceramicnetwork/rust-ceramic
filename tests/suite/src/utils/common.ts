'use strict'

import { CommonTestUtils } from '@ceramicnetwork/common-test-utils'


export interface Endpoint {
  host: string;
  port: number;
}

export function urlsToEndpoint(urls: string[]): Endpoint[] {
  return urls.map(url => {
    const parsed_url = new URL(url);
    return { host: parsed_url.hostname, port: parseInt(parsed_url.port, 10) };
  });
}

export const utilities = {
  valid: (exp: any) => {
    return exp !== undefined && exp !== null
  },
  success: (body?: any) => {
    const response = {
      statusCode: 200,
      body: body ? JSON.stringify(body) : 'ok',
      headers: {
        'Content-Type': 'application/json',
      },
    }
    console.log('response = ' + JSON.stringify(response))
    return response
  },
  failure: (error: Error) => {
    if (error === undefined) {
      error = new Error('unknown error occurred')
    }
    const response = {
      statusCode: 500,
      body: JSON.stringify(error),
      headers: {
        'Content-Type': 'application/json',
      },
    }
    console.log('response = ' + JSON.stringify(response))
    return response
  },
  delay: async (seconds: number): Promise<void> =>
    new Promise((resolve) => setTimeout(() => resolve(), seconds * 1000)),
  delayMs: async (ms: number): Promise<void> =>
    new Promise((resolve) => setTimeout(() => resolve(), ms)),
}

export class EventAccumulator<T> {
  #source: EventSource
  #parseEventData: (eventData: any) => T
  readonly allEvents: Set<T> = new Set()

  constructor(source: EventSource, parseEventData?: (event: MessageEvent) => T) {
    this.#source = source
    this.#parseEventData = parseEventData || ((eventData) => eventData.toString())
    this.start()
  }

  async waitForEvents(expected: Set<T>, timeoutMs?: number): Promise<void> {
    await CommonTestUtils.waitForConditionOrTimeout(async () => {
      const received = new Set(Array.from(this.allEvents).filter((event) => expected.has(event)))
      return received.size === expected.size
    }, timeoutMs)
  }

  stop() {
    this.#source.close()
  }

  start() {
    this.#source.addEventListener('message', (event) => {
      const parsedEvent = this.#parseEventData(event.data)
      this.allEvents.add(parsedEvent)
    })
  }
}

