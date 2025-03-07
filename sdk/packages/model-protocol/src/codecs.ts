import { SignedEvent, TimeEvent } from '@ceramic-sdk/events'
import {
  streamIDAsBytes,
  streamIDAsString,
  streamIDString,
} from '@ceramic-sdk/identifiers'
import { cid, didString } from '@didtools/codecs'
import addFormats from 'ajv-formats'
import Ajv from 'ajv/dist/2020.js'
import {
  Type,
  type TypeOf,
  array,
  boolean,
  identity,
  literal,
  nullCodec,
  optional,
  record,
  refinement,
  sparse,
  strict,
  string,
  tuple,
  union,
  unknown,
} from 'codeco'
import type { JSONSchema } from 'json-schema-typed/draft-2020-12'
import 'multiformats' // Import needed for TS reference
import 'ts-essentials' // Import needed for TS reference

import { MODEL } from './constants.js'

/** @internal */
export const ajv = new Ajv({
  strict: true,
  allErrors: true,
  allowMatchingProperties: false,
  ownProperties: false,
  unevaluated: false,
})
addFormats(ajv)

export type { JSONSchema } from 'json-schema-typed/draft-2020-12'

export type SchemaType =
  | JSONSchema.Boolean
  | JSONSchema.Integer
  | JSONSchema.Number
  | JSONSchema.String
  | JSONSchema.Array
  | JSONSchema.Object

/**
 * Create a codec for a specific JSON Schema type such as Object, String, etc.
 * @internal
 */
export function createSchemaType<T extends SchemaType>(
  type: T['type'],
  name: string,
): Type<T> {
  function isSchemaType(input: unknown): input is T {
    return (
      typeof input === 'object' && input != null && (input as T).type === type
    )
  }

  return new Type<T>(
    name,
    isSchemaType,
    (input, context) => {
      if (!isSchemaType(input)) {
        return context.failure(
          `Input is not a JSON schema of type: ${type as string}`,
        )
      }

      const isValid = ajv.validateSchema(input)
      // Remove schema from the Ajv instance's cache, otherwise the ajv cache grows unbounded
      ajv.removeSchema(input)

      return isValid
        ? context.success(input)
        : context.failure(`Validation Error: ${ajv.errorsText()}`)
    },
    identity,
  )
}

export const ObjectSchema = createSchemaType<JSONSchema.Object>(
  'object',
  'ObjectSchema',
)
export type ObjectSchema = TypeOf<typeof ObjectSchema>

export const optionalModelString = union([streamIDString, nullCodec])

/**
 * Metadata for a Model Stream
 */
export const ModelMetadata = strict(
  {
    /**
     * The DID that is allowed to author updates to this Model
     */
    controller: didString,
    /**
     * All Model streams have the same 'model' constant in their metadata. It is not a valid StreamID and cannot by loaded, but serves as a way to signal to indexers to index the set of all Models
     */
    model: refinement(streamIDAsString, (id) => id.equals(MODEL)),
  },
  'ModelMetadata',
)
export type ModelMetadata = TypeOf<typeof ModelMetadata>

/**
 * Represents the relationship between an instance of this model and the controller account.
 * 'list' means there can be many instances of this model for a single account. 'single' means
 * there can be only one instance of this model per account (if a new instance is created it
 * overrides the old one).
 */
export const ModelAccountRelation = union(
  [strict({ type: literal('list') }), strict({ type: literal('single') })],
  'ModelAccountRelation',
)
export type ModelAccountRelation = TypeOf<typeof ModelAccountRelation>

/**
 * Represents the relationship between an instance of this model and the controller account:
 * - 'list' means there can be many instances of this model for a single account
 * - 'single' means there can be only one instance of this model per account (if a new instance is created it
 * overrides the old one)
 * - 'none' means there can be no instance associated to an account (for interfaces notably)
 * - 'set' means there can be only one instance of this model per account and value of the specified content 'fields'
 *
 */
export const ModelAccountRelationV2 = union(
  [
    strict({ type: literal('list') }),
    strict({ type: literal('single') }),
    strict({ type: literal('none') }),
    strict({ type: literal('set'), fields: array(string) }),
  ],
  'ModelAccountRelationV2',
)
export type ModelAccountRelationV2 = TypeOf<typeof ModelAccountRelationV2>

/**
 * Identifies types of properties that are supported as relations by the indexing service.
 *
 * Currently supported types of relation properties:
 * - 'account': references a DID property
 * - 'document': references a StreamID property with associated 'model' the related document must use
 *
 */
export const ModelRelationDefinition = union(
  [
    strict({ type: literal('account') }),
    strict({ type: literal('document'), model: streamIDString }),
  ],
  'ModelRelationDefinition',
)
export type ModelRelationDefinition = TypeOf<typeof ModelRelationDefinition>

/**
 * Identifies types of properties that are supported as relations by the indexing service.
 *
 * Currently supported types of relation properties:
 * - 'account': references a DID property
 * - 'document': references a StreamID property with associated 'model' the related document must use if provided
 *
 */
export const ModelRelationDefinitionV2 = union(
  [
    strict({ type: literal('account') }),
    strict({ type: literal('document'), model: optionalModelString }),
  ],
  'ModelRelationDefinitionV2',
)
export type ModelRelationDefinitionV2 = TypeOf<typeof ModelRelationDefinitionV2>

/**
 * A mapping between model's property names and types of relation properties
 *
 * It indicates which properties of a model are relation properties and of what type
 */
export const ModelRelationsDefinition = record(
  string,
  ModelRelationDefinition,
  'ModelRelationsDefinition',
)
export type ModelRelationsDefinition = TypeOf<typeof ModelRelationsDefinition>

export const ModelRelationsDefinitionV2 = record(
  string,
  ModelRelationDefinitionV2,
  'ModelRelationsDefinitionV2',
)
export type ModelRelationsDefinitionV2 = TypeOf<
  typeof ModelRelationsDefinitionV2
>

export const ModelDocumentMetadataViewDefinition = union(
  [
    strict({ type: literal('documentAccount') }),
    strict({ type: literal('documentVersion') }),
  ],
  'ModelDocumentMetadataViewDefinition',
)
export type ModelDocumentMetadataViewDefinition = TypeOf<
  typeof ModelDocumentMetadataViewDefinition
>

export const ModelRelationViewDefinition = union(
  [
    strict({
      type: literal('relationDocument'),
      model: streamIDString,
      property: string,
    }),
    strict({
      type: literal('relationFrom'),
      model: streamIDString,
      property: string,
    }),
    strict({
      type: literal('relationCountFrom'),
      model: streamIDString,
      property: string,
    }),
  ],
  'ModelRelationViewDefinition',
)
export type ModelRelationViewDefinition = TypeOf<
  typeof ModelRelationViewDefinition
>

export const ModelRelationViewDefinitionV2 = union(
  [
    strict({
      type: literal('relationDocument'),
      model: optionalModelString,
      property: string,
    }),
    strict({
      type: literal('relationFrom'),
      model: streamIDString,
      property: string,
    }),
    strict({
      type: literal('relationCountFrom'),
      model: streamIDString,
      property: string,
    }),
    strict({
      type: literal('relationSetFrom'),
      model: streamIDString,
      property: string,
    }),
  ],
  'ModelRelationViewDefinitionV2',
)
export type ModelRelationViewDefinitionV2 = TypeOf<
  typeof ModelRelationViewDefinitionV2
>

/**
 * Identifies types of properties that are supported as view properties at DApps' runtime
 *
 * A view-property is one that is not stored in related MIDs' content, but is derived from their other properties
 *
 * Currently supported types of view properties:
 * - 'documentAccount': view properties of this type have the MID's controller DID as values
 * - 'documentVersion': view properties of this type have the MID's commit ID as values
 * - 'relationDocument': view properties of this type represent document relations identified by the given 'property' field
 * - 'relationFrom': view properties of this type represent inverse relations identified by the given 'model' and 'property' fields
 * - 'relationCountFrom': view properties of this type represent the number of inverse relations identified by the given 'model' and 'property' fields
 *
 */
export const ModelViewDefinition = union(
  [ModelDocumentMetadataViewDefinition, ModelRelationViewDefinition],
  'ModelViewDefinition',
)
export type ModelViewDefinition = TypeOf<typeof ModelViewDefinition>

/**
 * Identifies types of properties that are supported as view properties at DApps' runtime
 *
 * A view-property is one that is not stored in related MIDs' content, but is derived from their other properties
 *
 * Currently supported types of view properties:
 * - 'documentAccount': view properties of this type have the MID's controller DID as values
 * - 'documentVersion': view properties of this type have the MID's commit ID as values
 * - 'relationDocument': view properties of this type represent document relations identified by the given 'property' field
 * - 'relationFrom': view properties of this type represent inverse relations identified by the given 'model' and 'property' fields
 * - 'relationCountFrom': view properties of this type represent the number of inverse relations identified by the given 'model' and 'property' fields
 * - 'relationSetFrom': view properties of this type represent a single inverse relation identified by the given 'model' and 'property' fields for models using the SET account relation
 *
 */
export const ModelViewDefinitionV2 = union(
  [ModelDocumentMetadataViewDefinition, ModelRelationViewDefinitionV2],
  'ModelViewDefinitionV2',
)
export type ModelViewDefinitionV2 = TypeOf<typeof ModelViewDefinitionV2>

/**
 * A mapping between model's property names and types of view properties
 *
 * It indicates which properties of a model are view properties and of what type
 */
export const ModelViewsDefinition = record(
  string,
  ModelViewDefinition,
  'ModelViewDefinition',
)
export type ModelViewsDefinition = TypeOf<typeof ModelViewsDefinition>

/**
 * A mapping between model's property names and types of view properties
 *
 * It indicates which properties of a model are view properties and of what type
 */
export const ModelViewsDefinitionV2 = record(
  string,
  ModelViewDefinitionV2,
  'ModelViewDefinitionV2',
)
export type ModelViewsDefinitionV2 = TypeOf<typeof ModelViewsDefinitionV2>

export const ModelDefinitionV1 = sparse(
  {
    version: literal('1.0'),
    name: string,
    description: optional(string),
    schema: ObjectSchema,
    accountRelation: ModelAccountRelation,
    relations: optional(ModelRelationsDefinition),
    views: optional(ModelViewsDefinition),
  },
  'ModelDefinitionV1',
)
export type ModelDefinitionV1 = TypeOf<typeof ModelDefinitionV1>

export const ModelDefinitionV2 = sparse(
  {
    version: literal('2.0'),
    name: string,
    description: optional(string),
    interface: boolean,
    implements: array(streamIDString),
    schema: ObjectSchema,
    immutableFields: optional(array(string)),
    accountRelation: ModelAccountRelationV2,
    relations: optional(ModelRelationsDefinitionV2),
    views: optional(ModelViewsDefinitionV2),
  },
  'ModelDefinitionV2',
)
export type ModelDefinitionV2 = TypeOf<typeof ModelDefinitionV2>

/**
 * Contents of a Model Stream.
 */
export const ModelDefinition = union(
  [ModelDefinitionV1, ModelDefinitionV2],
  'ModelDefinition',
)
export type ModelDefinition = TypeOf<typeof ModelDefinition>

/**
 * Header of a Model Stream.
 */
export const ModelEventHeader = strict(
  {
    controllers: tuple([didString]),
    model: refinement(streamIDAsBytes, (id) => id.equals(MODEL)),
    sep: literal('model'),
  },
  'ModelEventHeader',
)
export type ModelEventHeader = TypeOf<typeof ModelEventHeader>

/**
 * Model Init event payload.
 */
export const ModelInitEventPayload = strict(
  {
    data: ModelDefinition,
    header: ModelEventHeader,
  },
  'ModelInitEventPayload',
)
export type ModelInitEventPayload = TypeOf<typeof ModelInitEventPayload>

/**
 * Model event: either a signed event or a time event.
 */
export const ModelEvent = union([SignedEvent, TimeEvent], 'ModelEvent')
export type ModelEvent = TypeOf<typeof ModelEvent>

/**
 * JSON patch operations.
 */

export const JSONPatchAddOperation = strict(
  {
    op: literal('add'),
    path: string,
    value: unknown,
  },
  'JSONPatchAddOperation',
)

export const JSONPatchRemoveOperation = strict(
  {
    op: literal('remove'),
    path: string,
  },
  'JSONPatchRemoveOperation',
)

export const JSONPatchReplaceOperation = strict(
  {
    op: literal('replace'),
    path: string,
    value: unknown,
  },
  'JSONPatchReplaceOperation',
)

export const JSONPatchMoveOperation = strict(
  {
    op: literal('move'),
    path: string,
    from: string,
  },
  'JSONPatchMoveOperation',
)

export const JSONPatchCopyOperation = strict(
  {
    op: literal('copy'),
    path: string,
    from: string,
  },
  'JSONPatchCopyOperation',
)

export const JSONPatchTestOperation = strict(
  {
    op: literal('test'),
    path: string,
    value: unknown,
  },
  'JSONPatchTestOperation',
)

export const JSONPatchOperation = union(
  [
    JSONPatchAddOperation,
    JSONPatchRemoveOperation,
    JSONPatchReplaceOperation,
    JSONPatchMoveOperation,
    JSONPatchCopyOperation,
    JSONPatchTestOperation,
  ],
  'JSONPatchOperation',
)
export type JSONPatchOperation = TypeOf<typeof JSONPatchOperation>

/**
 * Data event payload for a Model stream
 */
export const ModelDataEventPayload = sparse(
  {
    data: array(JSONPatchOperation),
    prev: cid,
    id: cid,
  },
  'ModelDataEventPayload',
)
export type ModelDataEventPayload = TypeOf<typeof ModelDataEventPayload>
