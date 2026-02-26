import type {
  BPTreeOrder,
  DataplyOptions
} from 'dataply'

export type Primitive = string | number | boolean | null;
export type JSONValue = Primitive | JSONValue[] | { [key: string]: JSONValue };

export type DocumentJSON = { [key: string]: JSONValue };
export type FlattenedDocumentJSON = { [key: string]: Primitive };

export interface DocumentDataplyInnerMetadata {
  magicString: string
  version: number
  createdAt: number
  updatedAt: number
  lastId: number
  indices: {
    [key: string]: [
      number,
      boolean | {
        type: 'fts',
        tokenizer: 'whitespace'
      } | {
        type: 'fts',
        tokenizer: 'ngram',
        gramSize: number
      }
    ]
  }
}

export interface DocumentDataplyMetadata {
  /**
   * The size of a page in bytes.
   */
  pageSize: number
  /**
   * The total number of pages in the dataply.
   */
  pageCount: number
  /**
   * The total number of data rows in the dataply.
   */
  rowCount: number
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
  or?: Partial<V>[]
  like?: string
  match?: string
}

export type DocumentDataplyQuery<T> = {
  [key in keyof T]?: T[key] | DocumentDataplyCondition<T[key]>
} & {
  [key: string]: any
}

/**
 * Query type restricted to indexed fields only
 */
export type DocumentDataplyIndexedQuery<
  T extends DocumentJSON,
  IC extends IndexConfig<T>
> = {
  [key in keyof IC]: key extends keyof FinalFlatten<DataplyDocument<T>>
  ? FinalFlatten<DataplyDocument<T>>[key] | DocumentDataplyCondition<FinalFlatten<DataplyDocument<T>>[key]>
  : never
} & DocumentDataplyQuery<{
  _id: number
}>

export interface DataplyTreeValue<T> {
  k: number
  v: T
}

/**
 * Options for querying documents.
 */
export type DocumentDataplyQueryOptions<
  T extends DocumentJSON,
  IC extends IndexConfig<T>
> = {
  /**
   * The maximum number of documents to return.
   */
  limit?: number
  /**
   * The number of documents to skip.
   */
  offset?: number
  /**
   * The field to order the results by.
   */
  orderBy?: ExtractIndexKeys<T, IC> | '_id'
  /**
   * The order to sort the results by.
   */
  sortOrder?: BPTreeOrder
}

/**
 * T가 객체인지 확인하고, 객체라면 하위 키를 재귀적으로 탐색합니다.
 */
type Prev = [never, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

/**
 * T가 객체인지 확인하고, 객체라면 하위 키를 재귀적으로 탐색합니다.
 * Depth 제한을 두어 "Type instantiation is excessively deep and possibly infinite" 에러를 방지합니다.
 */
export type DeepFlattenKeys<T, Prefix extends string = "", D extends number = 5> =
  [D] extends [0] ? never :
  T extends Primitive ? (Prefix extends `${infer P}.` ? P : never)
  : T extends readonly any[] ? (
    DeepFlattenKeys<T[number], `${Prefix}${number}.`, Prev[D]>
  )
  : T extends object ? {
    [K in keyof T & string]: NonNullable<T[K]> extends Primitive
    ? `${Prefix}${K}`
    : DeepFlattenKeys<NonNullable<T[K]>, `${Prefix}${K}.`, Prev[D]>
  }[keyof T & string]
  : never

/**
 * 경로 문자열(Path)을 기반으로 원본 객체(T)에서 타입을 찾아옵니다.
 * 배열 인덱스 접근 허용 (ex: tags.0)
 */
type GetTypeByPath<T, Path extends string> =
  T extends readonly (infer U)[]
  ? Path extends `${infer Key}.${infer Rest}`
  ? Key extends `${number}`
  ? GetTypeByPath<U, Rest>
  : Key extends keyof T
  ? GetTypeByPath<T[Key], Rest>
  : never
  : Path extends `${number}`
  ? U
  : Path extends keyof T
  ? T[Path]
  : never
  : Path extends `${infer Key}.${infer Rest}`
  ? Key extends keyof T
  ? GetTypeByPath<NonNullable<T[Key]>, Rest>
  : never
  : Path extends keyof T
  ? T[Path]
  : never

export type FinalFlatten<T> = {
  [P in DeepFlattenKeys<T>]: GetTypeByPath<T, P & string>
}

export type DocumentDataplyIndices<T extends DocumentJSON, IC extends IndexConfig<T>> = {
  [key in keyof IC & keyof FinalFlatten<T>]: GetTypeByPath<T, key>
}

/**
 * Index configuration type
 */
export type FTSConfig = {
  type: 'fts',
  tokenizer: 'whitespace'
} | {
  type: 'fts',
  tokenizer: 'ngram',
  gramSize: number
};

export type IndexConfig<T> = Partial<{
  [key in keyof FinalFlatten<T>]: boolean | FTSConfig
}>

/**
 * Extract index keys from IndexConfig
 */
export type ExtractIndexKeys<
  T extends DocumentJSON,
  IC extends IndexConfig<T>
> = keyof IC & keyof FinalFlatten<DataplyDocument<T>> & string

export interface DocumentDataplyOptions<
  T,
  IC extends IndexConfig<T> = IndexConfig<T>
> extends DataplyOptions {
  /**
   * Indices to create when initializing the database.
   * If not specified, no indices will be created.
   * If the value of the index is `true`, the index will be created for the already inserted data.
   * If the value of the index is `false`, the index will not be created for the already inserted data.
   */
  indices?: IC
}
