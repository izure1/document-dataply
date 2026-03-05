import type { DataplyTreeValue, Primitive } from '../../types'
import { ValueComparator } from 'dataply'

/**
 * 두 Primitive 값을 비교합니다.
 */
function compareDiff(a: Primitive, b: Primitive): number {
  if (typeof a !== 'string' && typeof b !== 'string') {
    return +(a as number) - +(b as number)
  }
  return (a + '').localeCompare(b + '')
}

/**
 * 두 v 값을 비교합니다. v는 Primitive 또는 Primitive[] (복합 인덱스)일 수 있습니다.
 * 배열인 경우 element-by-element로 비교합니다.
 */
function compareValue(a: Primitive | Primitive[], b: Primitive | Primitive[]): number {
  const aArr = Array.isArray(a)
  const bArr = Array.isArray(b)

  if (!aArr && !bArr) {
    return compareDiff(a as Primitive, b as Primitive)
  }

  // 배열 vs 배열: element-by-element 비교
  const aList = aArr ? a as Primitive[] : [a as Primitive]
  const bList = bArr ? b as Primitive[] : [b as Primitive]
  const len = Math.min(aList.length, bList.length)

  for (let i = 0; i < len; i++) {
    const diff = compareDiff(aList[i], bList[i])
    if (diff !== 0) return diff
  }

  return 0
}

export class DocumentValueComparator<T extends DataplyTreeValue<U>, U extends Primitive> extends ValueComparator<T> {
  primaryAsc(a: T, b: T): number {
    return compareValue(a.v, b.v)
  }

  asc(a: T, b: T): number {
    const diff = compareValue(a.v, b.v)
    return diff === 0 ? a.k - b.k : diff
  }

  match(value: T): string {
    return value.v + ''
  }
}
