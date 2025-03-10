import { bases } from 'multiformats/basics'
import { CID } from 'multiformats/cid'
import {
  MAX_BLOCK_SIZE,
  decodeMultibase,
  decodeMultibaseToJSON,
  decodeMultibaseToStreamID,
  restrictBlockSize,
} from '../src/utils.js'

describe('utils', () => {
  const TEST_CID = CID.parse(
    'bafyreih7ih3jx5uwwandvhoqyltr4vpl4ra47e4raeaejucdrvk6nl2kui',
  )

  test('restrictBlockSize() throws if the block size exceeds the maximum allowed', () => {
    expect(() => {
      restrictBlockSize(new Uint8Array(MAX_BLOCK_SIZE + 1), TEST_CID)
    }).toThrow(
      'bafyreih7ih3jx5uwwandvhoqyltr4vpl4ra47e4raeaejucdrvk6nl2kui commit size 256001 exceeds the maximum block size of 256000',
    )
    expect(() => {
      restrictBlockSize(new Uint8Array(MAX_BLOCK_SIZE), TEST_CID)
    }).not.toThrow()
  })

  describe('decodeMultibase', () => {
    it('should decode a valid Base64url-encoded string', () => {
      const input =
        'ueyJtZXRhZGF0YSI6eyJzaG91bGRJbmRleCI6dHJ1ZX0sImNvbnRlbnQiOnsiYm9keSI6IlRoaXMgaXMgYSBzaW1wbGUgbWVzc2FnZSJ9fQ'
      const payload = {
        metadata: { shouldIndex: true },
        content: { body: 'This is a simple message' },
      }
      const array = new TextEncoder().encode(JSON.stringify(payload))
      const result = decodeMultibase(input)

      expect(result).toEqual(array)
    })
    it('should decode a valid Base32-encoded string', () => {
      const payload = {
        metadata: { shouldIndex: true },
        content: { body: 'This is a simple message' },
      }
      const array = new TextEncoder().encode(JSON.stringify(payload))
      const encoded = bases.base32.encode(array)
      const result = decodeMultibase(encoded)

      expect(result).toEqual(array)
    })
  })
  describe('decodeMultibaseToJSON', () => {
    it('should decode a valid multibase-encoded string to JSON', () => {
      const input =
        'ueyJtZXRhZGF0YSI6eyJzaG91bGRJbmRleCI6dHJ1ZX0sImNvbnRlbnQiOnsiYm9keSI6IlRoaXMgaXMgYSBzaW1wbGUgbWVzc2FnZSJ9fQ'
      const payload = {
        metadata: { shouldIndex: true },
        content: { body: 'This is a simple message' },
      }
      const result = decodeMultibaseToJSON(input)

      expect(result).toEqual(payload)
    })
    it('should decode a valid multibase-encoded string to JSON', () => {
      const payload = {
        metadata: { shouldIndex: true },
        content: { body: 'This is a simple message' },
      }
      const array = new TextEncoder().encode(JSON.stringify(payload))
      const encoded = bases.base32.encode(array)
      const result = decodeMultibaseToJSON(encoded)

      expect(result).toEqual(payload)
    })
  })
  describe('decodeMultibaseToStreamID', () => {
    it('should decode a valid multibase-encoded string to StreamID', () => {
      const input = 'uzgEAAXESIA8og02Dnbwed_besT8M0YOnaZ-hrmMZaa7mnpdUL8jE'
      const stream =
        'k2t6wyfsu4pfx2cbha7xh9fsjvqr8b7g3w7365w627bup0l5qo020e2id4txvo'
      const result = decodeMultibaseToStreamID(input)
      expect(result.toString()).toEqual(stream)
    })
  })
})
