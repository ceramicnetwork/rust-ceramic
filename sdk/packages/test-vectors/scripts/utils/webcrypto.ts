import { WebcryptoProvider } from '@didtools/key-webcrypto'
import { DID } from 'dids'
import { getResolver } from 'key-did-resolver'

const PUBLIC_ED25519_JWK = {
  key_ops: ['verify'],
  ext: true,
  crv: 'Ed25519',
  x: '3ihGYza1Rc9I3nxiDDJisuhz4uOaPtUEkTPtEME3WWU',
  kty: 'OKP',
}
const PRIVATE_ED25519_JWK = {
  ...PUBLIC_ED25519_JWK,
  key_ops: ['sign'],
  d: 'r63eoVZ6sCW39-OcAKd3qQHd3ipJq9V52CmE5baTavc',
}

const PUBLIC_P256_JWK = {
  key_ops: ['verify'],
  ext: true,
  kty: 'EC',
  x: 'O-xQi9-EwhpwQT3bCxgzYoD4hmBtwZSourf9-lokltU',
  y: 'LqPnHu_70X9BE6AM2bVNtxncG7T0W44jSbvd7CbeKlo',
  crv: 'P-256',
}
const PRIVATE_P256_JWK = {
  ...PUBLIC_P256_JWK,
  key_ops: ['sign'],
  d: 'YMbt7DF46-IRKwgKJZcvLaBQYfz6nY4Ts7eAI8ybFXk',
}

const P256_IMPORT_PARAMS: EcKeyImportParams = {
  name: 'ECDSA',
  namedCurve: 'P-256',
}

async function importKey(
  jwk: JsonWebKey,
  algorithm: AlgorithmIdentifier | EcKeyImportParams,
  usage: KeyUsage,
): Promise<CryptoKey> {
  return await crypto.subtle.importKey('jwk', jwk, algorithm, true, [usage])
}

export async function getEd25519KeyPair(): Promise<CryptoKeyPair> {
  const [privateKey, publicKey] = await Promise.all([
    importKey(PRIVATE_ED25519_JWK, 'Ed25519', 'sign'),
    importKey(PUBLIC_ED25519_JWK, 'Ed25519', 'verify'),
  ])
  return { privateKey, publicKey }
}

export async function getP256KeyPair(): Promise<CryptoKeyPair> {
  const [privateKey, publicKey] = await Promise.all([
    importKey(PRIVATE_P256_JWK, P256_IMPORT_PARAMS, 'sign'),
    importKey(PUBLIC_P256_JWK, P256_IMPORT_PARAMS, 'verify'),
  ])
  return { privateKey, publicKey }
}

export async function getP256KeyDID(): Promise<DID> {
  const keyPair = await getP256KeyPair()
  const did = new DID({
    provider: new WebcryptoProvider(keyPair),
    resolver: getResolver(),
  })
  await did.authenticate()
  return did
}
