import { rustCrateVersion } from '../index'

test('returns native code version', () => {
  const r = rustCrateVersion()
  expect(r).toBeTruthy()
})
