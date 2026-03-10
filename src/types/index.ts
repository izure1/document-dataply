import type {
  BPTreeOrder,
  DataplyOptions
} from 'dataply'

export type Primitive = string | number | boolean | null
export type JSONValue = Primitive | JSONValue[] | { [key: string]: JSONValue }

export type DocumentJSON = { [key: string]: JSONValue }
export type FlattenedDocumentJSON = { [key: string]: Primitive }

/**
 * Index metadata config stored in DB metadata.
 * Used internally to persist index configuration.
 */
export type IndexMetaConfig = {
  type: 'btree'
  fields: string[]
} | {
  type: 'fts'
  fields: string
  tokenizer: 'whitespace'
} | {
  type: 'fts'
  fields: string
  tokenizer: 'ngram'
  gramSize: number
}

export interface DocumentDataplyInnerMetadata {
  magicString: string
  version: number
  createdAt: number
  updatedAt: number
  lastId: number
  schemeVersion: number
  indices: {
    [key: string]: [
      number,
      IndexMetaConfig
    ]
  }
  analysis?: number
}

/**
 * Analysis header row structure.
 * Maps analysis type names to their overflow row PKs.
 */
export interface AnalysisHeader {
  [type: string]: number
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
  /**
   * The usage of the dataply. It is calculated based on the remaining page capacity.
   * The value is between 0 and 1.
   */
  usage: number
  /**
   * The list of user-created index names (excludes internal '_id' index).
   */
  indices: string[]
  /**
   * The current scheme version of the database.
   */
  schemeVersion: number
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

export interface DataplyTreeValue<T> {
  k: number
  v: T
}

/**
 * Options for querying documents.
 */
export type DocumentDataplyQueryOptions = {
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
  orderBy?: string
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

export type FTSConfig = {
  type: 'fts',
  tokenizer: 'whitespace'
} | {
  type: 'fts',
  tokenizer: 'ngram',
  gramSize: number
}

/**
 * createIndex option types
 */
export type CreateIndexBTreeOption<T extends DocumentJSON> = {
  type: 'btree'
  fields: (DeepFlattenKeys<DataplyDocument<T>> & string)[]
}

export type CreateIndexFTSOption<T extends DocumentJSON> = {
  type: 'fts'
  fields: DeepFlattenKeys<DataplyDocument<T>> & string
  tokenizer: 'whitespace'
} | {
  type: 'fts'
  fields: DeepFlattenKeys<DataplyDocument<T>> & string
  tokenizer: 'ngram'
  gramSize: number
}

export type CreateIndexOption<T extends DocumentJSON> =
  CreateIndexBTreeOption<T> | CreateIndexFTSOption<T>

export interface DocumentDataplyOptions extends DataplyOptions {
  /**
   * The cron expression for the analysis schedule.
   * If not provided, default is '* *\/1 * * *' (every 1 hour)
   */
  analysisSchedule?: string
  /**
   * The sample size for the analysis.
   * If not provided, default is 1000
   */
  analysisSampleSize?: number
}
