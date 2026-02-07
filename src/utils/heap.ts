export type Comparator<T> = (a: T, b: T) => number

/**
 * A simple Binary Heap implementation.
 */
export class BinaryHeap<T> {
  private heap: T[] = []

  constructor(private comparator: Comparator<T>) { }

  get size(): number {
    return this.heap.length
  }

  peek(): T | undefined {
    return this.heap[0]
  }

  push(value: T): void {
    this.heap.push(value)
    this.bubbleUp(this.heap.length - 1)
  }

  pop(): T | undefined {
    if (this.size === 0) return undefined
    const top = this.heap[0]
    const bottom = this.heap.pop()!
    if (this.size > 0) {
      this.heap[0] = bottom
      this.sinkDown(0)
    }
    return top
  }

  /**
   * Replace the root element with a new value and re-heapify.
   * Faster than pop() followed by push().
   */
  replace(value: T): T | undefined {
    const top = this.heap[0]
    this.heap[0] = value
    this.sinkDown(0)
    return top
  }

  toArray(): T[] {
    return [...this.heap]
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      if (this.comparator(this.heap[index], this.heap[parentIndex]) >= 0) break
      [this.heap[index], this.heap[parentIndex]] = [this.heap[parentIndex], this.heap[index]]
      index = parentIndex
    }
  }

  private sinkDown(index: number): void {
    while (true) {
      let smallest = index
      const left = 2 * index + 1
      const right = 2 * index + 2

      if (left < this.size && this.comparator(this.heap[left], this.heap[smallest]) < 0) {
        smallest = left
      }
      if (right < this.size && this.comparator(this.heap[right], this.heap[smallest]) < 0) {
        smallest = right
      }

      if (smallest === index) break
      [this.heap[index], this.heap[smallest]] = [this.heap[smallest], this.heap[index]]
      index = smallest
    }
  }
}
