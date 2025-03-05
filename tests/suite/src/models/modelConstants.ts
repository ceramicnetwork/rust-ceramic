import { type ModelDefinition } from '@ceramicnetwork/stream-model'

export const LIST_MODEL_DEFINITION: ModelDefinition = {
  name: 'CDITModel',
  version: '1.0',
  schema: {
    type: 'object',
    additionalProperties: false,
    properties: {
      step: {
        type: 'integer',
        minimum: 0,
        maximum: 10000,
      },
    },
  },
  accountRelation: {
    type: 'list',
  },
}

export const SINGLE_MODEL_DEFINITION: ModelDefinition = {
  name: 'ceramic-tests-single-model',
  version: '1.0',
  schema: {
    type: 'object',
    additionalProperties: false,
    properties: {
      step: {
        type: 'integer',
        minimum: 0,
        maximum: 10000,
      },
    },
  },
  accountRelation: { type: 'single' },
}
