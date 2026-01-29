import { ValueComparator } from 'dataply'
import type { DataplyTreeValue, Primitive } from '../../types'

export class DocumentValueComparator<T extends DataplyTreeValue<U>, U extends Primitive> extends ValueComparator<T> {
  primaryAsc(a: T, b: T): number {
    if (typeof a.v !== 'string' || typeof b.v !== 'string') {
      return +(a.v as number) - +(b.v as number)
    }
    return a.v.localeCompare(b.v)
  }

  asc(a: T, b: T): number {
    if (typeof a.v !== 'string' || typeof b.v !== 'string') {
      const diff = +(a.v as number) - +(b.v as number)
      return diff === 0 ? a.k - b.k : diff
    }
    const diff = a.v.localeCompare(b.v)
    return diff === 0 ? a.k - b.k : diff
  }

  match(value: T): string {
    return value.v + ''
  }
}
