import { MODEL } from '../src'

test('MODEL constant', () => {
  expect(MODEL.bytes).toMatchSnapshot()
  expect(MODEL.toString()).toMatchSnapshot()
})
