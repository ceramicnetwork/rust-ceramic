import type { CAR, IBlock } from 'cartonne'

import { carFactory } from '../../src/index.ts'

export function createCAR(
  blocks: Iterable<IBlock>,
  createBlocks: Record<string, unknown>,
  meta: Record<string, unknown> = {},
): CAR {
  const car = carFactory.build()
  for (const block of blocks) {
    car.blocks.put(block)
  }
  const rootValue = { ...meta }
  for (const [key, value] of Object.entries(createBlocks)) {
    rootValue[key] = car.put(value)
  }
  car.put(rootValue, { isRoot: true })
  return car
}
