import type { CommitID } from '@ceramic-sdk/identifiers'
import type { DocumentMetadata } from '@ceramic-sdk/model-instance-protocol'

export type UnknownContent = Record<string, unknown>

export type DocumentState = {
  commitID: CommitID
  content: UnknownContent | null
  metadata: DocumentMetadata
}
