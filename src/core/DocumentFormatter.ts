import type { DocumentJSON, FlattenedDocumentJSON } from '../types'

export class DocumentFormatter<T extends DocumentJSON> {
  private flattenInternal(obj: any, parentKey: string = '', result: FlattenedDocumentJSON = {}): FlattenedDocumentJSON {
    for (const key in obj) {
      const newKey = parentKey ? `${parentKey}.${key}` : key
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        this.flattenInternal(obj[key], newKey, result)
      }
      else {
        result[newKey] = obj[key]
      }
    }
    return result
  }

  /**
   * returns flattened document
   * @param document
   * @returns
   */
  flattenDocument(document: T): FlattenedDocumentJSON {
    return this.flattenInternal(document, '', {})
  }
}
