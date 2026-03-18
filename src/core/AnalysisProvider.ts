import type { DocumentJSON } from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import type { Transaction, Logger, LoggerManager } from 'dataply'

/**
 * Abstract base class for analysis providers.
 * Subclasses should extend either RealtimeAnalysisProvider or IntervalAnalysisProvider.
 */
export abstract class AnalysisProvider<T extends DocumentJSON = DocumentJSON> {
  /** Overflow row PK assigned by AnalysisManager during initialization. */
  storageKey: number = -1

  private _logger?: Logger
  protected get logger(): Logger {
    if (!this._logger) {
      this._logger = this.loggerManager.create(`document-dataply:analysis:${this.name}`)
    }
    return this._logger
  }

  constructor(protected api: DocumentDataplyAPI<T>, protected loggerManager: LoggerManager) { }

  /**
   * Unique name of this analysis type (e.g. 'ftsTermCount').
   * Used as the key in the AnalysisHeader.
   */
  abstract readonly name: string

  /**
   * Load existing statistics data from a raw string.
   * Called during initialization to restore state from disk.
   * @param raw The raw string from the overflow row, or null if no data exists yet
   * @param tx Optional transaction
   */
  abstract load(raw: string | null, tx: Transaction): Promise<void>

  /**
   * Serialize the current statistics data to a raw string for storage.
   * @param tx Optional transaction
   */
  abstract serialize(tx: Transaction): Promise<string>
}
