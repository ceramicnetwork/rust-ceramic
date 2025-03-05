import type { FlightSqlClient } from '@ceramic-sdk/flight-sql-client'
import { base64 } from 'multiformats/bases/base64'
import type { CID } from 'multiformats/cid'
import ContainerWrapper from './withContainer.js'

const DEFAULT_PORT = 5101
const DEFAULT_FLIGHT_SQL_PORT = 5102

const DEFAULT_ENVIRONMENT = {
  CERAMIC_ONE_BIND_ADDRESS: '0.0.0.0:5101',
  CERAMIC_ONE_EXPERIMENTAL_FEATURES: 'true',
  CERAMIC_ONE_FLIGHT_SQL_BIND_ADDRESS: '0.0.0.0:5102',
  CERAMIC_ONE_LOG_FORMAT: 'single-line',
  CERAMIC_ONE_NETWORK: 'in-memory',
  CERAMIC_ONE_AGGREGATOR: 'true',
  CERAMIC_ONE_OBJECT_STORE_URL: 'file://./generated',
}

export type EnvironmentOptions = {
  debug?: boolean // default to false
  image?: string // default to "public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest-debug"
  containerName?: string // default to "ceramic-one"
  apiPort: number // default to 5101
  internalApiPort?: number // default to 5101
  internalFlightSqlPort?: number // default to 5102
  flightSqlPort: number // default to 5102
  externalPort?: number
  connectTimeoutSeconds?: number // default to 10
  // defaults to api port if not specified
  testPort?: number
  environment?: Record<string, string> // Ceramic daemon environment variables
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export default class CeramicOneContainer {
  #container: ContainerWrapper

  private constructor(container: ContainerWrapper) {
    this.#container = container
  }

  static async healthFn(port: number): Promise<boolean> {
    try {
      const res = await fetch(`http://localhost:${port}/ceramic/liveness`)

      if (res.status === 200) {
        return true
      }
      return false
    } catch (err) {
      return false
    }
  }

  static async isResponding(port: number) {
    let retries = 10
    while (retries > 0) {
      if (await CeramicOneContainer.healthFn(port)) {
        return
      }
      retries -= 1
      await delay(1000)
    }
    console.error(
      `Failed to verify container was up on port ${port}. tests will fail`,
    )
    return
  }

  static async startContainer(
    options: EnvironmentOptions,
  ): Promise<CeramicOneContainer> {
    const container = await ContainerWrapper.startContainer({
      debug: options.debug ?? false,
      image:
        options.image ??
        'public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest-debug',
      // TODO: would be nice to have 1 container for all tests rather than 1 each since we get rate errors pulling
      // refreshImage: true, // pull new images
      containerName:
        options.containerName ??
        `ceramic-${Math.random().toString(36).slice(6)}`,
      apiPort: options.apiPort ?? DEFAULT_PORT,
      internalApiPort: options.internalApiPort ?? DEFAULT_PORT,
      flightSqlPort: options.flightSqlPort || DEFAULT_FLIGHT_SQL_PORT,
      internalFlightSqlPort:
        options.internalFlightSqlPort || DEFAULT_FLIGHT_SQL_PORT,
      connectTimeoutSeconds: options.connectTimeoutSeconds ?? 10,
      environment: { ...DEFAULT_ENVIRONMENT, ...options.environment },
      detached: true,
    })

    // we do this because the container connects on the port but the api doesn't respond right away so the tests can fail
    // could add/improve `EnvironmentOptions.testConnection` handling but I didn't get something right so just doing this
    await CeramicOneContainer.isResponding(options.apiPort)
    return new CeramicOneContainer(container)
  }

  async teardown(): Promise<void> {
    await this.#container.kill()
  }
}

// Wait for count events states
export async function waitForEventState(
  flightClient: FlightSqlClient,
  event_cid: CID,
) {
  await flightClient.preparedQuery(
    'SELECT event_state_order FROM event_states_feed WHERE event_cid = $event_cid LIMIT 1',
    new Array(['$event_cid', event_cid.toString(base64.encoder)]),
  )
}
