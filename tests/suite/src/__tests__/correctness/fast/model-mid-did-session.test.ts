import { beforeAll, describe, expect, test } from '@jest/globals'
import { AuthMethod, AuthMethodOpts, Cacao, SiweMessage } from '@didtools/cacao'
import { AccountId } from 'caip'
import { DIDSession } from 'did-session'
import { randomString } from '@stablelib/random'
import { normalizeAccountId } from '@ceramicnetwork/common'
import { Wallet as Signer } from 'ethers'
import { type ClientOptions, createFlightSqlClient, FlightSqlClient } from '@ceramic-sdk/flight-sql-client'
import { CeramicClient } from '@ceramic-sdk/http-client'
import { ModelClient } from '@ceramic-sdk/model-client'
import type { ModelDefinition } from '@ceramic-sdk/model-protocol'
import { StreamID } from '@ceramic-sdk/identifiers'
import { ModelInstanceClient } from '@ceramic-sdk/model-instance-client'
import { DID } from 'dids'

import { randomDID } from '../../../utils/didHelper'
import { urlsToEndpoint } from '../../../utils/common'
import { waitForEventState } from '../../../utils/rustCeramicHelpers'

const CeramicUrls = String(process.env.CERAMIC_URLS).split(',')
const CeramicFlightUrls = String(process.env.CERAMIC_FLIGHT_URLS).split(',')
const CeramicFlightEndpoints = urlsToEndpoint(CeramicFlightUrls)

const FLIGHT_OPTIONS: ClientOptions = {
    headers: new Array(),
    username: undefined,
    password: undefined,
    token: undefined,
    tls: false,
    host: CeramicFlightEndpoints[0].host,
    port: CeramicFlightEndpoints[0].port,
}

const getCacao = (
    accountId: AccountId,
    signer: Signer
): AuthMethod => async (opts: AuthMethodOpts) => {
    const now = new Date()
    const oneWeekLater = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000)
    const normalizedAccount = normalizeAccountId(accountId)

    const siweMessage = new SiweMessage({
        domain: new URL(CeramicUrls[0]).hostname,
        address: normalizedAccount.address,
        statement: opts.statement ?? 'Give this application access to some of your data on Ceramic',
        uri: opts.uri,
        version: '1',
        nonce: opts.nonce ?? randomString(10),
        issuedAt: now.toISOString(),
        expirationTime: opts.expirationTime ?? oneWeekLater.toISOString(),
        chainId: normalizedAccount.chainId.reference,
        resources: opts.resources,
    })
    siweMessage.signature = await signer.signMessage(siweMessage.signMessage())

    return Cacao.fromSiweMessage(siweMessage)
}

export const authorizedSessionDid = async (
    signer: Signer,
    resources: string[],
) => {
    const address = await signer.getAddress()
    const chainId = 'eip155:11155111'
    const caipAccountId = new AccountId({ address, chainId })
    const authMethod: AuthMethod = getCacao(caipAccountId, signer)
    const session = await DIDSession.authorize(
        authMethod,
        { resources }
    )
    return session.did
}

describe('create/update stream using session did', () => {
    let flightClient: FlightSqlClient
    let client: CeramicClient
    let modelInstanceClient: ModelInstanceClient
    let modelStream: StreamID

    const testSigner = new Signer('0x4c0883a69102937d6231471b5dbb6204eaa7f9b0c8f2d6f8e1c5f3a2b6c7d8e9')
    let authenticatedDID1: DID
    let authenticatedDID2: DID

    beforeAll(async () => {
        flightClient = await createFlightSqlClient(FLIGHT_OPTIONS)

        client = new CeramicClient({
            url: CeramicUrls[0]
        })

        modelInstanceClient = new ModelInstanceClient({
            ceramic: client,
            did: await randomDID()
        })

        const modelClient = new ModelClient({
            ceramic: client,
            did: await randomDID()
        })
        
        const testModel: ModelDefinition = {
            version: '2.0',
            name: 'TestModel',
            description: 'List Test model',
            accountRelation: { type: 'list' },
            interface: false,
            implements: [],
            schema: {
                type: 'object',
                properties: {
                    test: { type: 'string', maxLength: 10 },
                },
                additionalProperties: false,
            },
        }

        modelStream = await modelClient.createDefinition(testModel)
        // Use the flightsql stream behavior to ensure the events states have been process before querying their states.
        await waitForEventState(flightClient, modelStream.cid)

        authenticatedDID1 = await authorizedSessionDid(
            testSigner,
            [`ceramic://*?model=${modelStream.toString()}`],
        )
        authenticatedDID2 = await authorizedSessionDid(
            testSigner,
            [`ceramic://*?model=${modelStream.toString()}`],
        )
    }, 10000)
    
    test('create/update stream', async () => {
        const init = await modelInstanceClient.createInstance({
            controller: authenticatedDID1,
            model: modelStream,
            content: { test: 'hello' },
            shouldIndex: true,
        })

        await waitForEventState(flightClient, init.cid)
        
        const streamInit = await modelInstanceClient.getDocumentState(init.baseID)
        let controller = streamInit.metadata.controller
        const signerAddress = (await testSigner.getAddress()).toLowerCase()

        expect(controller).toEqual(authenticatedDID1.parent)
        expect(controller!.replace(/did:pkh.*:/, '').toLowerCase()).toEqual(signerAddress)
        
        const streamUpdate = await modelInstanceClient.updateDocument({
            controller: authenticatedDID2,
            streamID: init.baseID.toString(),
            newContent: { test: 'world' },
            shouldIndex: true,
        })

        await waitForEventState(flightClient, streamUpdate.commitID.cid)
    })
})
