import { SignedEvent, TimeEvent } from '@ceramic-sdk/events'
import { streamIDAsBytes, streamIDAsString } from '@ceramic-sdk/identifiers'
import {
  cid,
  didString,
  uint8ArrayAsBase64,
  uint8array,
} from '@didtools/codecs'
import {
  type OutputOf,
  type TypeOf,
  array,
  boolean,
  literal,
  nullCodec,
  optional,
  sparse,
  strict,
  string,
  tuple,
  union,
  unknown,
  unknownRecord,
} from 'codeco'
import 'multiformats' // Import needed for TS reference
import 'ts-essentials' // Import needed for TS reference

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
 * Init event header for a ModelInstanceDocument Stream
 */
export const DocumentInitEventHeader = sparse(
  {
    controllers: tuple([didString]),
    model: streamIDAsBytes,
    modelVersion: optional(cid),
    sep: literal('model'),
    unique: optional(uint8array),
    context: optional(streamIDAsBytes),
    shouldIndex: optional(boolean),
  },
  'DocumentInitEventHeader',
)
export type DocumentInitEventHeader = TypeOf<typeof DocumentInitEventHeader>

/**
 * Init event payload for a non-deterministic ModelInstanceDocument Stream
 */
export const DataInitEventPayload = sparse(
  {
    data: unknownRecord,
    header: DocumentInitEventHeader,
  },
  'DataInitEventPayload',
)
export type DataInitEventPayload = TypeOf<typeof DataInitEventPayload>

/**
 * Init event payload for a deterministic ModelInstanceDocument Stream
 */
export const DeterministicInitEventPayload = sparse(
  {
    data: nullCodec,
    header: DocumentInitEventHeader,
  },
  'DeterministicInitEventPayload',
)
export type DeterministicInitEventPayload = TypeOf<
  typeof DeterministicInitEventPayload
>
export type EncodedDeterministicInitEventPayload = OutputOf<
  typeof DeterministicInitEventPayload
>

/**
 * Init event payload for a ModelInstanceDocument Stream
 */
export const DocumentInitEventPayload = union(
  [DeterministicInitEventPayload, DataInitEventPayload],
  'DocumentInitEventPayload',
)
export type DocumentInitEventPayload = TypeOf<typeof DocumentInitEventPayload>

/**
 * Data event header for a ModelInstanceDocument Stream
 */
export const DocumentDataEventHeader = sparse(
  {
    shouldIndex: optional(boolean),
    modelVersion: optional(cid),
  },
  'DocumentDataEventHeader',
)
export type DocumentDataEventHeader = TypeOf<typeof DocumentDataEventHeader>

/**
 * Data event payload for a ModelInstanceDocument Stream
 */
export const DocumentDataEventPayload = sparse(
  {
    data: array(JSONPatchOperation),
    prev: cid,
    id: cid,
    header: optional(DocumentDataEventHeader),
  },
  'DocumentDataEventPayload',
)
export type DocumentDataEventPayload = TypeOf<typeof DocumentDataEventPayload>

/**
 * Metadata for a ModelInstanceDocument Stream
 */
export const DocumentMetadata = sparse(
  {
    /**
     * The DID that is allowed to author updates to this ModelInstanceDocument
     */
    controller: didString,
    /**
     * The StreamID of the Model that this ModelInstanceDocument belongs to.
     */
    model: streamIDAsString,
    /**
     * Unique bytes
     */
    unique: optional(uint8ArrayAsBase64),
    /**
     * The "context" StreamID for this ModelInstanceDocument.
     */
    context: optional(streamIDAsString),
    /**
     * Whether the stream should be indexed or not.
     */
    shouldIndex: optional(boolean),
  },
  'DocumentMetadata',
)
export type DocumentMetadata = TypeOf<typeof DocumentMetadata>
export type EncodedDocumentMetadata = OutputOf<typeof DocumentMetadata>

/**
 * Document event: either a deterministic init event payload, a signed event or a time event.
 */
export const DocumentEvent = union(
  [
    DeterministicInitEventPayload,
    SignedEvent, // non-deterministic init or data payload
    TimeEvent,
  ],
  'DocumentEvent',
)
export type DocumentEvent = OutputOf<typeof DocumentEvent>
