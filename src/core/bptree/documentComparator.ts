import type { DataplyTreeValue, Primitive } from '../../types'
import { ValueComparator } from 'dataply'

/**
 * 두 Primitive 값을 비교합니다.
 * null < boolean < number < string 순서로 비교합니다.
 */
function comparePrimitive(a: Primitive, b: Primitive): number {
  if (a === b) return 0
  if (a === null) return -1
  if (b === null) return 1
  if (typeof a !== typeof b) {
    // 타입 우선순위: boolean(0) < number(1) < string(2)
    const typeOrder = (v: Primitive): number =>
      typeof v === 'boolean' ? 0 : typeof v === 'number' ? 1 : 2
    return typeOrder(a) - typeOrder(b)
  }
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b)
  }
  return +(a as number) - +(b as number)
}

/**
 * 두 v 값을 비교합니다. v는 Primitive 또는 Primitive[] (복합 인덱스)일 수 있습니다.
 * 배열인 경우 element-by-element로 비교합니다.
 */
function compareValue(a: Primitive | Primitive[], b: Primitive | Primitive[]): number {
  const aArr = Array.isArray(a)
  const bArr = Array.isArray(b)

  if (!aArr && !bArr) {
    return comparePrimitive(a as Primitive, b as Primitive)
  }

  // 배열 vs 배열: element-by-element 비교
  const aList = aArr ? a as Primitive[] : [a as Primitive]
  const bList = bArr ? b as Primitive[] : [b as Primitive]
  const len = Math.min(aList.length, bList.length)

  for (let i = 0; i < len; i++) {
    const diff = comparePrimitive(aList[i], bList[i])
    if (diff !== 0) return diff
  }
  return aList.length - bList.length
}

/**
 * 접두사 매칭(prefix match) 비교.
 * 짧은 쪽 배열이 긴 쪽의 접두사와 모두 일치하면 0을 반환합니다.
 * 복합 인덱스에서 primaryEqual 등 부분 필드 검색을 지원합니다.
 */
function comparePrimaryValue(a: Primitive | Primitive[], b: Primitive | Primitive[]): number {
  const aArr = Array.isArray(a)
  const bArr = Array.isArray(b)

  if (!aArr && !bArr) {
    return comparePrimitive(a as Primitive, b as Primitive)
  }

  const aList = aArr ? a as Primitive[] : [a as Primitive]
  const bList = bArr ? b as Primitive[] : [b as Primitive]
  const len = Math.min(aList.length, bList.length)

  for (let i = 0; i < len; i++) {
    const diff = comparePrimitive(aList[i], bList[i])
    if (diff !== 0) return diff
  }
  // prefix match: 짧은 쪽이 긴 쪽의 접두사와 일치하면 0 반환
  return 0
}

export class DocumentValueComparator<T extends DataplyTreeValue<U>, U extends Primitive> extends ValueComparator<T> {
  primaryAsc(a: T, b: T): number {
    return comparePrimaryValue(a.v, b.v)
  }

  asc(a: T, b: T): number {
    const diff = compareValue(a.v, b.v)
    return diff === 0 ? a.k - b.k : diff
  }

  match(value: T): string {
    if (Array.isArray(value.v)) {
      return value.v[0] + ''
    }
    return value.v + ''
  }
}
