import { AnalysisProvider } from '../AnalysisProvider'
import type { FlattenedDocumentJSON, DocumentJSON } from '../../types'

export class FTSTermCount<T extends DocumentJSON = DocumentJSON> extends AnalysisProvider<T> {
  readonly name = 'ftsTermCount'
  readonly triggerMode = 'interval' as const

  private termCount: Map<string, number> = new Map()

  async serialize(): Promise<string> {
    return 'test'
  }

  async load(data: string | null): Promise<void> {
  }

  async onInsert(documents: FlattenedDocumentJSON[]): Promise<void> {
  }

  async onDelete(documents: FlattenedDocumentJSON[]): Promise<void> {
  }

  async onUpdate(pairs: { oldDocument: FlattenedDocumentJSON, newDocument: FlattenedDocumentJSON }[]): Promise<void> {
  }
}
