import { CeramicClient } from '@ceramic-sdk/http-client'
import { jest } from '@jest/globals'
import { DID } from 'dids'
import { StreamClient } from '../src/index.js'

describe('StreamClient', () => {
  describe('ceramic getter', () => {
    test('returns the CeramicClient instance set in constructor', () => {
      const ceramic = new CeramicClient({ url: 'http://localhost:5101' })
      const client = new StreamClient({ ceramic })
      expect(client.ceramic).toBe(ceramic)
    })

    test('returns a CeramicClient instance using the provided URL', () => {
      const client = new StreamClient({ ceramic: 'http://localhost:5101' })
      expect(client.ceramic).toBeInstanceOf(CeramicClient)
    })
  })

  describe('getDID() method', () => {
    test('throws if no DID is provided or set in the constructor', () => {
      const client = new StreamClient({ ceramic: 'http://localhost:5101' })
      expect(() => client.getDID()).toThrow('Missing DID')
    })

    test('returns the DID set in the constructor', () => {
      const did = new DID()
      const client = new StreamClient({ ceramic: 'http://localhost:5101', did })
      expect(client.getDID()).toBe(did)
    })

    test('returns the DID provided as argument', async () => {
      const did = new DID()
      const client = new StreamClient({
        ceramic: 'http://localhost:5101',
        did: new DID(),
      })
      expect(client.getDID(did)).toBe(did)
    })
  })
  describe('getStreamState() method', () => {
    test('fetches the state of a stream by its ID', async () => {
      const streamId = 'streamId123'
      const mockStreamState = {
        id: streamId,
        controller: 'did:example:123',
        data: 'someEncodedData',
        event_cid: 'someCid',
        dimensions: {},
      }

      // Mock CeramicClient and its API
      const mockGet = jest.fn(() =>
        Promise.resolve({
          data: mockStreamState,
          error: null,
        }),
      )
      const mockCeramicClient = {
        api: { GET: mockGet },
      } as unknown as CeramicClient

      const client = new StreamClient({ ceramic: mockCeramicClient })
      const state = await client.getStreamState(streamId)

      expect(state).toEqual(mockStreamState)
      expect(mockGet).toHaveBeenCalledWith('/streams/{stream_id}', {
        params: { path: { stream_id: streamId } },
      })
    })

    test('throws an error if the stream is not found', async () => {
      const streamId = 'invalidStreamId'
      const mockError = { message: 'Stream not found' }

      // Mock CeramicClient and its API
      const mockGet = jest.fn(() =>
        Promise.resolve({
          data: null,
          error: mockError,
        }),
      )
      const mockCeramicClient = {
        api: { GET: mockGet },
      } as unknown as CeramicClient

      const client = new StreamClient({ ceramic: mockCeramicClient })

      await expect(client.getStreamState(streamId)).rejects.toThrow(
        'Stream not found',
      )
      expect(mockGet).toHaveBeenCalledWith('/streams/{stream_id}', {
        params: { path: { stream_id: streamId } },
      })
    })
  })
})
