/**
 * 이벤트 루프를 막지 않고 대량의 데이터를 처리하기 위한 유틸리티 클래스
 */
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

  constructor(targetMs: number = 5, alpha: number = 0.5) {
    this.chunkSize = 0
    this.targetMs = targetMs
    this.alpha = alpha
    this.ewmaMs = null
  }

  /**
   * EWMA 평활화 계수를 사용하여 평균 처리 시간을 업데이트합니다.
   */
  private updateEstimate(elapsed: number, count: number): void {
    const msPerItem = elapsed / count
    this.ewmaMs = this.ewmaMs === null
      ? msPerItem
      : this.alpha * msPerItem + (1 - this.alpha) * this.ewmaMs
  }

  /**
   * 현재 chunk size를 업데이트합니다.
   */
  private nextChunkSize(): number {
    if (!this.ewmaMs || this.ewmaMs === 0) return this.chunkSize
    const next = Math.floor(this.targetMs / this.ewmaMs)
    return Math.max(1, next)
  }

  /**
   * 주어진 items를 chunk로 분할하여 처리합니다.
   */
  async processInChunks<T>(items: T[], processFn: (chunk: T[]) => Promise<void>): Promise<void> {
    let i = 0
    let len = items.length
    this.chunkSize = Math.floor(items.length / 100 * 5) // 초기값은 전체의 5%를 청크 사이즈로 설정
    while (i < len) {
      const chunk = items.slice(i, i + this.chunkSize)
      const count = chunk.length
      const start = performance.now()

      await processFn(chunk)

      const elapsed = performance.now() - start
      this.updateEstimate(elapsed, count)
      this.chunkSize = this.nextChunkSize()

      i += count
      await new Promise(setImmediate)
    }
  }
}
