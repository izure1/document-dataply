/**
 * 양보(Yield) 함수
 * Node.js의 마이크로태스크 큐 고갈(Event loop starvation)을 방지하기 위해 
 * 의도적으로 짧은 비동기 대기(Macrotask)를 발생시킵니다.
 */
export function yieldLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve))
}
