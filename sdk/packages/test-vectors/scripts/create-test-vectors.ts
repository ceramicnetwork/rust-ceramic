import {
  InitEventPayload,
  type SignedEvent,
  signEvent,
  signedEventToCAR,
} from '@ceramic-sdk/events'
import { CommitID, StreamID } from '@ceramic-sdk/identifiers'
import {
  createDataEventPayload,
  createInitHeader,
  getDeterministicInitEvent,
} from '@ceramic-sdk/model-instance-client'
import { getStreamID } from '@ceramic-sdk/model-instance-protocol'
import type { AuthMethod, Cacao } from '@didtools/cacao'
import type { IBlock } from 'cartonne'
import type { CreateJWSOptions, DID } from 'dids'

import type { ControllerType } from '../src/index.ts'

import { createCAR } from './utils/car.ts'
import {
  createCapabilityDID,
  createExpiredCapabilityDID,
  keyDID,
} from './utils/did.ts'
import { getAuthMethod as getEthereumAuth } from './utils/ethereum.ts'
import { writeCARFile } from './utils/fs.ts'
import { getAuthMethod as getSolanaAuth } from './utils/solana.ts'
import { getAuthMethod as getWebAuthnAuth } from './utils/webauthn.ts'
import { getP256KeyDID } from './utils/webcrypto.ts'

function changeEventSignature(event: SignedEvent): SignedEvent {
  const [firstSignature, ...otherSignatures] = event.jws.signatures
  return {
    ...event,
    jws: {
      ...event.jws,
      signatures: [
        {
          ...firstSignature,
          signature: `${firstSignature.signature.slice(0, -4)}AAAA`,
        },
        ...otherSignatures,
      ],
    },
  }
}

function changeCapabilitySignature(cacao: Cacao): Cacao {
  // biome-ignore lint/style/noNonNullAssertion: existing value
  const signature = cacao.s!
  return { ...cacao, s: { ...signature, s: `${signature.s.slice(0, -4)}AAAA` } }
}

const MODEL_ID =
  'k2t6wz4z9kggqqnbn3vqggh2z4fe9jctr9vvbc1em2yho0qnzzrtzz1n2hr4lq'
const model = StreamID.fromString(MODEL_ID)

type ControllerCommon = {
  id: string
  signer: DID
}

type ControllerWithoutCapability = ControllerCommon & { withCapability: false }

type ControllerWithCapability = ControllerCommon & {
  withCapability: true
  expiredSigner: DID
  noResourceSigner: DID
  expectedModelSigner: DID
  otherModelSigner: DID
}

type Controller = ControllerWithoutCapability | ControllerWithCapability

async function createCapabilityController(
  authMethod: AuthMethod,
): Promise<ControllerWithCapability> {
  const [
    signer,
    expiredSigner,
    noResourceSigner,
    expectedModelSigner,
    otherModelSigner,
  ] = await Promise.all([
    createCapabilityDID(authMethod, { resources: ['ceramic://*'] }),
    createExpiredCapabilityDID(authMethod, { resources: ['ceramic://*'] }),
    createCapabilityDID(authMethod, { resources: [] }),
    createCapabilityDID(authMethod, {
      resources: [`ceramic://*?model=${MODEL_ID}`],
    }),
    createCapabilityDID(authMethod, {
      resources: [
        'ceramic://*?model=k2t6wz4z9kggqqnbn3vqggh2z4fe9jctr9vvbc1em2yho0qnzzrtzz1n2hr4lr',
      ],
    }),
  ])
  return {
    withCapability: true,
    id: signer.id,
    signer,
    expiredSigner,
    noResourceSigner,
    expectedModelSigner,
    otherModelSigner,
  }
}

const controllerFactories = {
  'key-ecdsa-p256': async () => {
    const signer = await getP256KeyDID()
    return { withCapability: false, id: signer.id, signer }
  },
  'key-ed25519': () => {
    return { withCapability: false, id: keyDID.id, signer: keyDID }
  },
  'pkh-ethereum': async () => {
    const authMethod = await getEthereumAuth()
    return await createCapabilityController(authMethod)
  },
  'pkh-solana': async () => {
    const authMethod = await getSolanaAuth()
    return await createCapabilityController(authMethod)
  },
  'pkh-webauthn': async () => {
    const authMethod = await getWebAuthnAuth()
    return await createCapabilityController(authMethod)
  },
} satisfies Record<ControllerType, () => Controller | Promise<Controller>>

for (const [controllerType, createController] of Object.entries(
  controllerFactories,
)) {
  // Create controller
  const controller = await createController()

  // Deterministic (init) event
  const validDeterministicEvent = getDeterministicInitEvent(
    model,
    controller.id,
  )

  // Signed init event
  const validInitPayload = InitEventPayload.encode({
    data: { test: true },
    header: createInitHeader({
      controller: controller.id,
      model,
      unique: new Uint8Array([0, 1, 2, 3]),
    }),
  })
  const validInitEvent = await signEvent(controller.signer, validInitPayload)
  const validInitCAR = signedEventToCAR(validInitEvent)

  const invalidInitSignatureCAR = signedEventToCAR(
    changeEventSignature(validInitEvent),
  )

  // Data event
  const streamID = getStreamID(validInitCAR.roots[0])
  const validDataPayload = createDataEventPayload(
    CommitID.fromStream(streamID),
    [{ op: 'add', path: '/update', value: true }],
  )
  const validDataEvent = await signEvent(controller.signer, validDataPayload)
  const validDataCAR = signedEventToCAR(validDataEvent)

  const invalidDataSignatureCAR = signedEventToCAR(
    changeEventSignature(validDataEvent),
  )

  const carBlocks: Array<IBlock> = [
    ...validInitCAR.blocks,
    ...validDataCAR.blocks,
    ...invalidInitSignatureCAR.blocks,
    ...invalidDataSignatureCAR.blocks,
  ]
  const carMeta: Record<string, unknown> = {
    controller: controller.id,
    model: model.bytes,
    validInitEvent: validInitCAR.roots[0],
    validDataEvent: validDataCAR.roots[0],
    invalidInitEventSignature: invalidInitSignatureCAR.roots[0],
    invalidDataEventSignature: invalidDataSignatureCAR.roots[0],
  }

  if (controller.withCapability) {
    async function addEvents(
      signer: DID,
      initKey: string,
      dataKey: string,
      options?: CreateJWSOptions,
    ) {
      const initEvent = await signEvent(signer, validInitPayload, options)
      const initCAR = signedEventToCAR(initEvent)
      carBlocks.push(...initCAR.blocks)
      carMeta[initKey] = initCAR.roots[0]

      const dataEvent = await signEvent(signer, validDataPayload, options)
      const dataCAR = signedEventToCAR(dataEvent)
      carBlocks.push(...dataCAR.blocks)
      carMeta[dataKey] = dataCAR.roots[0]
    }

    // Events with expired capabilities
    await addEvents(
      controller.expiredSigner,
      'expiredInitEventCapability',
      'expiredDataEventCapability',
    )

    // Events with altered capability signatures
    await addEvents(
      controller.signer,
      'invalidInitEventCapabilitySignature',
      'invalidDataEventCapabilitySignature',
      {
        capability: changeCapabilitySignature(controller.signer.capability),
      } as unknown as CreateJWSOptions,
    )

    // Events with no allowed resource
    await addEvents(
      controller.noResourceSigner,
      'invalidInitEventCapabilityNoResource',
      'invalidDataEventCapabilityNoResource',
    )

    // Events with other model resource
    await addEvents(
      controller.otherModelSigner,
      'invalidInitEventCapabilityOtherModel',
      'invalidDataEventCapabilityOtherModel',
    )

    // Events with exact model resource (no wildcard)
    await addEvents(
      controller.expectedModelSigner,
      'validInitEventCapabilityExactModel',
      'validDataEventCapabilityExactModel',
    )
  }

  // Write CAR file
  const controllerCAR = createCAR(
    carBlocks,
    { validDeterministicEvent, validInitPayload, validDataPayload },
    carMeta,
  )
  await writeCARFile(controllerType as ControllerType, controllerCAR)
}
