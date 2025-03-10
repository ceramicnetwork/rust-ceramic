import { AccountId } from 'caip'
import type {
  EIP1193Parameters,
  EIP1193Provider,
  Hex,
  WalletRpcSchema,
} from 'viem'
import { privateKeyToAccount } from 'viem/accounts'

/**
 * CAIP2 for ethereum, used in CAIP10 (acountId)
 */
const CHAIN_NAMESPACE = 'eip155'

type RequestParameters = EIP1193Parameters<WalletRpcSchema>

export function createProvider(privateKey: Hex): EIP1193Provider {
  const account = privateKeyToAccount(privateKey)

  return {
    async request({ method, params }: RequestParameters) {
      switch (method) {
        case 'eth_chainId':
          return '1' // Mainnet
        case 'eth_requestAccounts':
          return [account.address]
        case 'personal_sign': {
          const [message, address] = params as [string, string]
          if (address !== account.address.toLowerCase()) {
            throw new Error(`Invalid address provided: ${address}`)
          }
          return await account.signMessage({ message })
        }
        default:
          throw new Error(`Unsupported method: ${method}`)
      }
    },
  } as unknown as EIP1193Provider
}

export async function getAccount(
  provider: EIP1193Provider,
): Promise<AccountId> {
  const [accounts, chain] = await Promise.all([
    provider.request({ method: 'eth_requestAccounts' }),
    provider.request({ method: 'eth_chainId' }),
  ])
  return new AccountId({
    address: accounts[0].toLowerCase(),
    chainId: `${CHAIN_NAMESPACE}:${Number.parseInt(chain, 16)}`,
  })
}
