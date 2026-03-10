export class DeadlineChunker {
  /**
   * 이벤트 루프를 막을 최대 허용 시간
   */
  private targetMs: number
  /**
   * 현재 chunk size
   */
  private chunkSize: number
  /**
   * Exponential Weighted Moving Average
   */
  private ewmaMs: number | null
  /**
   * EWMA 평활화 계수
   */
  private alpha: number

  constructor(targetMs = 14) {
    this.targetMs = targetMs
    this.chunkSize = 50
    this.ewmaMs = null
    this.alpha = 0.3
  }

  /**
   * EWMA 평활화 계수를 사용하여 평균 처리 시간을 업데이트합니다.
   */
  _updateEstimate(elapsed: number, count: number): void {
    const msPerItem = elapsed / count
    this.ewmaMs = this.ewmaMs === null
      ? msPerItem
      : this.alpha * msPerItem + (1 - this.alpha) * this.ewmaMs
  }

  /**
   * 현재 chunk size를 업데이트합니다.
   */
  _nextChunkSize(): number {
    if (!this.ewmaMs || this.ewmaMs === 0) return this.chunkSize
    const next = Math.floor(this.targetMs / this.ewmaMs)
    return Math.max(1, Math.min(next, 5000))
  }

  /**
   * 주어진 items를 chunk로 분할하여 처리합니다.
   */
  async processInChunks<T>(items: T[], processFn: (chunk: T[]) => Promise<void>): Promise<void> {
    let i = 0
    let len = items.length
    while (i < len) {
      const chunk = items.slice(i, i + this.chunkSize)
      const start = performance.now()

      await processFn(chunk)

      const elapsed = performance.now() - start
      this._updateEstimate(elapsed, chunk.length)
      this.chunkSize = this._nextChunkSize()

      i += chunk.length
      await new Promise(resolve => setImmediate(resolve))
    }
  }
}
