import { mkdir, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { CAR } from 'cartonne'

import { type ControllerType, getCARPath } from '../../src/index.ts'

export async function writeCARFile(
  controllerType: ControllerType,
  car: CAR,
): Promise<string> {
  const filePath = getCARPath(controllerType)
  await mkdir(dirname(filePath), { recursive: true })
  await writeFile(filePath, car.bytes)
  return filePath
}
