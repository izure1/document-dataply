import type { DataplyOptions, SerializeStrategyHead, BPTreeCondition } from 'dataply'

export type Primitive = string | number | boolean | null;
export type JSONValue = Primitive | JSONValue[] | { [key: string]: JSONValue };

export type DocumentJSON = { [key: string]: JSONValue };
export type FlattenedDocumentJSON = { [key: string]: Primitive };

export interface DocumentDataplyMetadata {
  magicString: string
  version: number
  createdAt: number
  updatedAt: number
  lastId: number
  treeHeads: Record<string, SerializeStrategyHead>
}

export type DataplyDocumentBase = { _id: number }

export type DataplyDocument<T extends DocumentJSON> = DataplyDocumentBase & T

export type DocumentDataplyCondition<V> = {
  lt?: Partial<V>
  lte?: Partial<V>
  gt?: Partial<V>
  gte?: Partial<V>
  equal?: Partial<V>
  notEqual?: Partial<V>
  like?: Partial<V>
}

export type DocumentDataplyQuery<T> = {
  [key in keyof T]: T[key] | DocumentDataplyCondition<T[key]>
}

export interface DataplyTreeValue<T> {
  k: number
  v: T
}

/**
 * T가 객체인지 확인하고, 객체라면 하위 키를 재귀적으로 탐색합니다.
 */
type DeepFlattenKeys<T, Prefix extends string = ""> = T extends object
  ? {
    [K in keyof T & string]: NonNullable<T[K]> extends object
    // 값이 객체라면 계속 파고듭니다. (끝에 .을 붙여서 전달)
    ? DeepFlattenKeys<NonNullable<T[K]>, `${Prefix}${K}.`>
    // 값이 객체가 아니라면 최종 경로 문자열을 반환합니다.
    : `${Prefix}${K}`
  }[keyof T & string]
  : Prefix extends `${infer P}.` ? P : ""

/**
 * 경로 문자열(Path)을 기반으로 원본 객체(T)에서 타입을 찾아옵니다.
 */
type GetTypeByPath<T, Path extends string> = Path extends `${infer Key}.${infer Rest}`
  ? Key extends keyof T
  ? GetTypeByPath<NonNullable<T[Key]>, Rest>
  : never
  : Path extends keyof T
  ? T[Path]
  : never

export type FinalFlatten<T> = {
  [P in DeepFlattenKeys<T>]: GetTypeByPath<T, P & string>
}

export interface DocumentDataplyOptions extends DataplyOptions {

}
