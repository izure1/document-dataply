import type { FlattenedDocumentJSON, DocumentJSON } from '../types'
import { AnalysisProvider } from './AnalysisProvider'

/**
 * Abstract base class for realtime analysis providers.
 * Mutation hooks (onInsert, onDelete, onUpdate) are called on every mutation
 * and the result is persisted immediately.
 */
export abstract class RealtimeAnalysisProvider<T extends DocumentJSON = DocumentJSON> extends AnalysisProvider<T> {
  /**
   * Called when documents are inserted.
   * @param documents The flattened documents that were inserted
   */
  abstract onInsert(documents: FlattenedDocumentJSON[]): Promise<void>

  /**
   * Called when documents are deleted.
   * @param documents The flattened documents that were deleted
   */
  abstract onDelete(documents: FlattenedDocumentJSON[]): Promise<void>

  /**
   * Called when documents are updated.
   * @param pairs Array of { oldDocument, newDocument } pairs
   */
  abstract onUpdate(pairs: { oldDocument: FlattenedDocumentJSON, newDocument: FlattenedDocumentJSON }[]): Promise<void>
}
