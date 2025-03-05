// MIT license sourced from https://github.com/ForbesLindesay/atdatabases/blob/master/packages/with-container/src/index.ts

import type { ChildProcess } from 'node:child_process'
import { connect } from 'node:net'
import spawn from 'cross-spawn'
import { spawnBuffered } from 'modern-spawn'

export interface Options {
  debug: boolean
  image: string
  containerName: string
  apiPort: number
  internalApiPort: number
  flightSqlPort: number
  internalFlightSqlPort: number
  connectTimeoutSeconds: number
  testPort?: number
  environment?: { [key: string]: string }
  /**
   * By default, we check if the image already exists
   * before pulling it. We only pull if there is no
   * existing image. This is faster, but means we don't
   * get updates to the image.
   */
  refreshImage?: boolean
  detached?: boolean
  enableDebugInstructions?: string
}

export async function imageExists(options: Options): Promise<boolean> {
  const stdout = await spawnBuffered(
    'docker',
    ['images', '--format', '{{json .}}'],
    {
      debug: options.debug,
    },
  ).getResult('utf8')
  const existingImages = stdout
    .trim()
    .split('\n')
    .map((str) => {
      try {
        return JSON.parse(str)
      } catch (ex) {
        console.warn(`Unable to parse: ${str}`)
        return null
      }
    })
    .filter((n) => n != null)
  const [Repository, Tag] = options.image.split(':')
  return existingImages.some(
    (i) => i.Repository === Repository && (!Tag || i.Tag === Tag),
  )
}
export async function pullDockerImage(options: Options) {
  if (
    !options.refreshImage &&
    /.+\:.+/.test(options.image) &&
    (await imageExists(options))
  ) {
    console.warn(
      `${options.image} already pulled (use 'refreshImage' to refresh)`,
    )
    return
  }
  console.log(`Pulling Docker Image ${options.image}`)
  await spawnBuffered('docker', ['pull', options.image], {
    debug: options.debug,
  }).getResult()
}

export function startDockerContainer(options: Options) {
  const env = options.environment || {}
  const envArgs: string[] = []
  for (const key of Object.keys(env)) {
    envArgs.push('--env')
    envArgs.push(`${key}=${env[key]}`)
  }
  return spawn(
    'docker',
    [
      'run',
      '--platform',
      'linux/x86_64', // our default ceramic-one build
      '--name',
      options.containerName,
      // '-t', // terminate when sent SIGTERM
      // '--rm', // automatically remove when container is killed
      '-p', // forward appropriate port
      `${options.flightSqlPort}:${options.internalFlightSqlPort}`,
      '-p',
      `${options.apiPort}:${options.internalApiPort}`,
      ...(options.detached ? ['--detach'] : []),
      // set enviornment variables
      ...envArgs,
      options.image,
    ],
    {
      stdio: options.debug ? 'inherit' : 'ignore',
    },
  )
}

export async function waitForDatabaseToStart(options: Options) {
  await new Promise<void>((resolve, reject) => {
    let finished = false
    const timeout = setTimeout(() => {
      finished = true
      reject(
        new Error(
          `Unable to connect to database after ${
            options.connectTimeoutSeconds
          } seconds.${
            options.enableDebugInstructions
              ? ` ${options.enableDebugInstructions}`
              : ''
          }`,
        ),
      )
    }, options.connectTimeoutSeconds * 1000)
    function test() {
      const testPort = options.testPort || options.apiPort
      console.log(`Waiting for ${options.containerName} on port ${testPort}...`)
      testConnection(testPort).then(
        (isConnected) => {
          if (finished) {
            console.log(
              `Connected to ${options.containerName} on port ${testPort}!`,
            )
            return
          }
          if (isConnected) {
            finished = true
            clearTimeout(timeout)
            setTimeout(resolve, 1000)
          } else {
            setTimeout(test, 500)
          }
        },
        (err) => {
          reject(err)
        },
      )
    }
    test()
  })
}
async function testConnection(port: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const connection = connect(port)
      .on('error', () => {
        resolve(false)
      })
      .on('connect', () => {
        connection.end()
        resolve(true)
      })
  })
}
async function killOldContainers(
  options: Pick<Options, 'debug' | 'containerName'>,
) {
  await spawnBuffered('docker', ['kill', options.containerName], {
    debug: options.debug,
  }) // do not check exit code as there may not be a container to kill
  await spawnBuffered('docker', ['rm', options.containerName], {
    debug: options.debug,
  }) // do not check exit code as there may not be a container to remove
}

export default class ContainerWrapper {
  proc: ChildProcess
  opts: Options

  private constructor(proc: ChildProcess, opts: Options) {
    this.proc = proc
    this.opts = opts
  }

  static async startContainer(opts: Options): Promise<ContainerWrapper> {
    if (Number.isNaN(opts.connectTimeoutSeconds)) {
      throw new Error('connectTimeoutSeconds must be a valid integer.')
    }

    if (typeof opts.apiPort !== 'number') {
      throw new Error('Expected external api port to be a number')
    }
    if (typeof opts.flightSqlPort !== 'number') {
      throw new Error('Expected external flight SQL port to be a number')
    }

    await Promise.all([pullDockerImage(opts), killOldContainers(opts)])

    console.log(`Starting Docker Container ${opts.containerName}`)
    const proc = startDockerContainer(opts)

    try {
      await waitForDatabaseToStart(opts)
      console.log('connected to container successfully!')
    } catch (e) {
      console.error(`failed to ping container: ${e}`)
      throw e
    }

    return new ContainerWrapper(proc, opts)
  }

  async kill() {
    await killOldContainers(this.opts)
  }
}
