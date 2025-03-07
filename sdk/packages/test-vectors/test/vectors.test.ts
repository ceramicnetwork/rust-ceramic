import {
  type SignedEvent,
  type SignedEventContainer,
  eventFromCAR,
  signedEventToContainer,
} from '@ceramic-sdk/events'
import { StreamID } from '@ceramic-sdk/identifiers'
import {
  DocumentDataEventPayload,
  DocumentInitEventPayload,
} from '@ceramic-sdk/model-instance-protocol'
import { Cacao } from '@didtools/cacao'
import { createDID } from '@didtools/key-did'
import { WebauthnAuth } from '@didtools/key-webauthn'
import { getEIP191Verifier } from '@didtools/pkh-ethereum'
import { getSolanaVerifier } from '@didtools/pkh-solana'
import type { CAR } from 'cartonne'

// Inject mock WebAuthn authenticator
import '../scripts/utils/webauthn.ts'

import {
  type ArchiveRootContentCommon,
  type ArchiveRootContentWithCapability,
  type ArchiveRootContentWithoutCapability,
  loadCAR,
} from '../src/index.ts'

const EXPECTED_EXPIRED_DATE = new Date(Date.UTC(2000, 0, 1, 0)).toISOString()

const verifier = createDID()

const [
  keyP256CAR,
  keyEd25519CAR,
  pkhEthereumCAR,
  pkhSolanaCAR,
  pkhWebAuthnCAR,
] = await Promise.all([
  loadCAR('key-ecdsa-p256'),
  loadCAR('key-ed25519'),
  loadCAR('pkh-ethereum'),
  loadCAR('pkh-solana'),
  loadCAR('pkh-webauthn'),
])

// Tests applying to any DID type

describe.each([
  ['key-ecdsa-p256', keyP256CAR],
  ['key-ed25519', keyEd25519CAR],
  ['pkh-ethereum', pkhEthereumCAR],
  ['pkh-solana', pkhSolanaCAR],
  ['pkh-webauthn', pkhWebAuthnCAR],
])('common checks using %s', (controllerType: string, car: CAR) => {
  const root = car.get(car.roots[0]) as ArchiveRootContentCommon

  test('valid deterministic init event', () => {
    const cid = root.validDeterministicEvent
    const event = eventFromCAR(DocumentInitEventPayload, car, cid)
    expect(event).toBeDefined()
  })

  test('invalid signature of signed init event', async () => {
    const event = eventFromCAR(
      DocumentInitEventPayload,
      car,
      root.invalidInitEventSignature,
    ) as SignedEvent
    expect(event.jws.link.toString()).toBe(root.validInitPayload.toString())

    await expect(async () => {
      await signedEventToContainer(verifier, DocumentInitEventPayload, event)
    }).rejects.toThrow('invalid_signature: Signature invalid for JWT')
  })

  test('invalid signature of signed data event', async () => {
    const event = eventFromCAR(
      DocumentDataEventPayload,
      car,
      root.invalidDataEventSignature,
    ) as SignedEvent
    expect(event.jws.link.toString()).toBe(root.validDataPayload.toString())

    await expect(async () => {
      await signedEventToContainer(verifier, DocumentDataEventPayload, event)
    }).rejects.toThrow('invalid_signature: Signature invalid for JWT')
  })
})

// Tests applying only to DIDs signing payloads directly

describe.each([
  ['key-ecdsa-p256', keyP256CAR],
  ['key-ed25519', keyEd25519CAR],
])('direct signatures using %s', (controllerType: string, car: CAR) => {
  const root = car.get(car.roots[0]) as ArchiveRootContentWithoutCapability

  test('valid signed init event', async () => {
    const event = eventFromCAR(
      DocumentInitEventPayload,
      car,
      root.validInitEvent,
    ) as SignedEvent
    expect(event.jws.link.toString()).toBe(root.validInitPayload.toString())

    const container = await signedEventToContainer(
      verifier,
      DocumentInitEventPayload,
      event,
    )
    expect(container.signed).toBe(true)

    const did = container.verified.kid.split('#')[0]
    expect(did).toBe(root.controller)
  })

  test('valid data event', async () => {
    const event = eventFromCAR(
      DocumentDataEventPayload,
      car,
      root.validDataEvent,
    ) as SignedEvent
    expect(event.jws.link.toString()).toBe(root.validDataPayload.toString())

    const container = await signedEventToContainer(
      verifier,
      DocumentDataEventPayload,
      event,
    )
    expect(container.signed).toBe(true)

    const did = container.verified.kid.split('#')[0]
    expect(did).toBe(root.controller)
  })
})

// Tests applying only to DIDs using CACAO

describe.each([
  ['pkh-ethereum', pkhEthereumCAR, getEIP191Verifier()],
  ['pkh-solana', pkhSolanaCAR, getSolanaVerifier()],
  ['pkh-webauthn', pkhWebAuthnCAR, WebauthnAuth.getVerifier()],
])(
  'CACAO signatures using %s',
  (controllerType: string, car: CAR, verifiers) => {
    const root = car.get(car.roots[0]) as ArchiveRootContentWithCapability
    const expectedModel = StreamID.fromBytes(root.model)

    describe.each([
      ['Init', DocumentInitEventPayload],
      ['Data', DocumentDataEventPayload],
    ])('%s events', (type: string, payloadCodec) => {
      function getEvent(prefix: string, suffix = ''): SignedEvent {
        const event = eventFromCAR(
          payloadCodec,
          car,
          root[`${prefix}${type}Event${suffix}` as keyof typeof root],
        ) as SignedEvent
        expect(event.jws.link.toString()).toBe(
          root[`valid${type}Payload`].toString(),
        )
        return event
      }

      async function getContainer(
        event: SignedEvent,
      ): Promise<SignedEventContainer<typeof payloadCodec>> {
        const container = await signedEventToContainer(
          verifier,
          payloadCodec,
          event,
        )
        expect(container.signed).toBe(true)
        return container as unknown as SignedEventContainer<typeof payloadCodec>
      }

      test('valid capability: wildcard ceramic resource', async () => {
        const event = getEvent('valid')
        const container = await getContainer(event)

        // biome-ignore lint/style/noNonNullAssertion: existing value
        const capability = await Cacao.fromBlockBytes(container.cacaoBlock!)
        const verified = await verifier.verifyJWS(event.jws, {
          capability,
          issuer: root.controller,
          verifiers,
        })
        expect(verified).toBeDefined()
      })

      test('valid capability: exact ceramic model resource', async () => {
        const event = getEvent('valid', 'CapabilityExactModel')
        const container = await getContainer(event)

        // biome-ignore lint/style/noNonNullAssertion: existing value
        const capability = await Cacao.fromBlockBytes(container.cacaoBlock!)
        const resources = capability.p.resources ?? []
        expect(resources).toHaveLength(1)
        expect(resources[0]).toBe(
          `ceramic://*?model=${expectedModel.toString()}`,
        )
      })

      test('expired capability', async () => {
        const event = getEvent('expired', 'Capability')
        const container = await getContainer(event)

        // biome-ignore lint/style/noNonNullAssertion: existing value
        const capability = await Cacao.fromBlockBytes(container.cacaoBlock!)
        expect(capability.p.exp).toBe(EXPECTED_EXPIRED_DATE)
      })

      test('invalid capability: signature', async () => {
        const event = getEvent('invalid', 'CapabilitySignature')
        const container = await getContainer(event)

        // biome-ignore lint/style/noNonNullAssertion: existing value
        const capability = await Cacao.fromBlockBytes(container.cacaoBlock!)
        await expect(async () => {
          await Cacao.verify(capability, { verifiers })
        }).rejects.toThrow()
      })

      test('invalid capability: no resource', async () => {
        const event = getEvent('invalid', 'CapabilityNoResource')
        const container = await getContainer(event)

        // biome-ignore lint/style/noNonNullAssertion: existing value
        const capability = await Cacao.fromBlockBytes(container.cacaoBlock!)
        expect(capability.p.resources).toHaveLength(0)
      })

      test('invalid capability: other model resource', async () => {
        const event = getEvent('invalid', 'CapabilityOtherModel')
        const container = await getContainer(event)

        // biome-ignore lint/style/noNonNullAssertion: existing value
        const capability = await Cacao.fromBlockBytes(container.cacaoBlock!)
        const resources = capability.p.resources ?? []
        expect(resources).toHaveLength(1)
        expect(resources[0]).not.toBe(
          `ceramic://*?model=${expectedModel.toString()}`,
        )
      })
    })
  },
)
