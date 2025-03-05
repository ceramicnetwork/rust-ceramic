import {
  type AuthMethod,
  type AuthMethodOpts,
  type Cacao,
  CacaoBlock,
} from '@didtools/cacao'
import { getAuthenticatedDID } from '@didtools/key-did'
import {
  type CreateJWSOptions,
  DID,
  type DagJWS,
  type DagJWSResult,
} from 'dids'
import { CID } from 'multiformats/cid'

const VALID_EXPIRATION_TIME = new Date(9999, 0).toISOString()
const INVALID_EXPIRATION_TIME = new Date(2000, 0).toISOString()

export const keyDID = await getAuthenticatedDID(new Uint8Array(32))

export type CustomCreateJWSOptions = CreateJWSOptions & {
  capability?: Cacao
}

export class CustomCapabilityDID extends DID {
  get capability(): Cacao {
    return this._capability
  }

  // Extend base createJWS() method add capability support in options
  async createJWS<T extends string | Record<string, unknown>>(
    payload: T,
    options: CustomCreateJWSOptions = {},
  ): Promise<DagJWS> {
    if (this._client == null) throw new Error('No provider available')
    if (this._id == null) throw new Error('DID is not authenticated')
    // Use capability from options when provided
    const capability = options.capability ?? this._capability
    if (capability) {
      const cacaoBlock = await CacaoBlock.fromCacao(capability)
      const capCID = CID.asCID(cacaoBlock.cid)
      if (!capCID) {
        throw new Error(
          'Capability CID of the JWS cannot be set to the capability payload cid as they are incompatible',
        )
      }
      options.protected = options.protected || {}
      options.protected.cap = `ipfs://${capCID?.toString()}`
    }
    const { jws } = await this._client.request('did_createJWS', {
      did: this._id,
      ...options,
      payload,
    })
    return jws
  }

  // Extend base createDagJWS() method add capability support in options
  async createDagJWS(
    payload: Record<string, unknown>,
    options: CustomCreateJWSOptions = {},
  ): Promise<DagJWSResult> {
    const result = await super.createDagJWS(payload, options)
    if (options.capability != null) {
      const cacaoBlock = await CacaoBlock.fromCacao(options.capability)
      result.cacaoBlock = cacaoBlock.bytes
    }
    return result
  }
}

export async function createCapabilityDID(
  authMethod: AuthMethod,
  authParams: AuthMethodOpts = {},
): Promise<CustomCapabilityDID> {
  const capability = await authMethod({
    domain: 'test',
    expirationTime: VALID_EXPIRATION_TIME,
    uri: keyDID.id,
    ...authParams,
  })
  const did = new CustomCapabilityDID({
    provider: keyDID._client?.connection,
    resolver: keyDID._resolver,
    capability,
    parent: keyDID._parentId,
  })
  await did.authenticate()
  return did
}

export class ExpiredCapabilityDID extends DID {
  // Extend base createJWS() method to remove expiry check
  async createJWS<T extends string | Record<string, unknown>>(
    payload: T,
    options: CreateJWSOptions = {},
  ): Promise<DagJWS> {
    if (this._client == null) throw new Error('No provider available')
    if (this._id == null) throw new Error('DID is not authenticated')
    if (this._capability) {
      const cacaoBlock = await CacaoBlock.fromCacao(this._capability)
      const capCID = CID.asCID(cacaoBlock.cid)
      if (!capCID) {
        throw new Error(
          'Capability CID of the JWS cannot be set to the capability payload cid as they are incompatible',
        )
      }
      options.protected = options.protected || {}
      options.protected.cap = `ipfs://${capCID?.toString()}`
    }
    const { jws } = await this._client.request('did_createJWS', {
      did: this._id,
      ...options,
      payload,
    })
    return jws
  }
}

export async function createExpiredCapabilityDID(
  authMethod: AuthMethod,
  authParams: AuthMethodOpts = {},
): Promise<DID> {
  const capability = await authMethod({
    domain: 'test',
    expirationTime: INVALID_EXPIRATION_TIME,
    uri: keyDID.id,
    ...authParams,
  })
  const did = new ExpiredCapabilityDID({
    provider: keyDID._client?.connection,
    resolver: keyDID._resolver,
    capability,
    parent: keyDID._parentId,
  })
  await did.authenticate()
  return did
}
