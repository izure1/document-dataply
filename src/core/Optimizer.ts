import type { DataplyTreeValue, DocumentDataplyQuery, DocumentDataplyCondition } from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import type { FTSTermCount } from './analysis/FTSTermCount'
import { BPTreeAsync } from 'dataply'
import { tokenize } from '../utils/tokenizer'

/**
 * 선택도 기본값 (= B+Tree 스캔 비용)
 * 값이 낮을수록 인덱스 스캔이 효율적임
 */
const SELECTIVITY = {
  /** O(log N) 포인트 룩업 */
  EQUAL: 0.01,
  /** 양쪽 바운드(gte+lte) 범위 스캔 */
  BOUNDED_RANGE: 0.33,
  /** 한쪽 바운드(gte 또는 lte)만 있을 때, 중간부터 풀스캔 */
  HALF_RANGE: 0.5,
  /** Or 조건: B+Tree 내부 풀스캔 */
  OR: 0.9,
  /** Like 조건: B+Tree 내부 풀스캔 */
  LIKE: 0.9,
  /** 알 수 없는 조건 */
  UNKNOWN: 0.9,
  /** FTS 통계 없을 때 보수적 추정 */
  FTS_DEFAULT: 0.5,
  /** 정렬 비용 가중치 (orderBy 미지원 시) */
  SORT_PENALTY: 0.3,
  /** 인메모리 정렬이 유의미해지는 임계 문서 수 */
  SORT_THRESHOLD: 10_000,
} as const

export class Optimizer<T extends Record<string, any>> {
  constructor(private api: DocumentDataplyAPI<T>) { }

  /**
   * B-Tree 타입 인덱스의 선택도를 평가하고 트리에 부여할 조건을 산출합니다.
   */
  evaluateBTreeCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    indexName: string,
    config: any,
    query: Partial<DocumentDataplyQuery<V>>,
    queryFields: Set<string>,
    treeTx: BPTreeAsync<string | number, V>,
    orderByField?: string
  ) {
    const primaryField = config.fields[0]
    if (!queryFields.has(primaryField)) return null

    const builtCondition: Record<string, any> = {}
    let selectivity = 1.0
    let isConsecutive = true
    const coveredFields: string[] = []
    const compositeVerifyFields: string[] = []
    const startValues: any[] = []
    const endValues: any[] = []
    let startOperator: string | null = null
    let endOperator: string | null = null

    for (let i = 0, len = config.fields.length; i < len; i++) {
      const field = config.fields[i]

      if (!queryFields.has(field)) {
        isConsecutive = false
        continue
      }

      coveredFields.push(field)

      if (isConsecutive) {
        const cond = query[field as keyof typeof query] as any
        if (cond !== undefined) {
          let isBounded = false

          if (typeof cond !== 'object' || cond === null) {
            selectivity *= SELECTIVITY.EQUAL
            startValues.push(cond)
            endValues.push(cond)
            startOperator = 'primaryGte'
            endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryEqual' in cond || 'equal' in cond) {
            const val = cond.primaryEqual?.v ?? cond.equal?.v ?? cond.primaryEqual ?? cond.equal
            selectivity *= SELECTIVITY.EQUAL
            startValues.push(val)
            endValues.push(val)
            startOperator = 'primaryGte'
            endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryGte' in cond || 'gte' in cond) {
            const val = cond.primaryGte?.v ?? cond.gte?.v ?? cond.primaryGte ?? cond.gte
            selectivity *= SELECTIVITY.HALF_RANGE
            isConsecutive = false
            startValues.push(val)
            startOperator = 'primaryGte'
            if (endValues.length > 0) endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryGt' in cond || 'gt' in cond) {
            const val = cond.primaryGt?.v ?? cond.gt?.v ?? cond.primaryGt ?? cond.gt
            selectivity *= SELECTIVITY.HALF_RANGE
            isConsecutive = false
            startValues.push(val)
            startOperator = 'primaryGt'
            if (endValues.length > 0) endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryLte' in cond || 'lte' in cond) {
            const val = cond.primaryLte?.v ?? cond.lte?.v ?? cond.primaryLte ?? cond.lte
            selectivity *= SELECTIVITY.HALF_RANGE
            isConsecutive = false
            endValues.push(val)
            endOperator = 'primaryLte'
            if (startValues.length > 0) startOperator = 'primaryGte'
            isBounded = true
          }
          else if ('primaryLt' in cond || 'lt' in cond) {
            const val = cond.primaryLt?.v ?? cond.lt?.v ?? cond.primaryLt ?? cond.lt
            selectivity *= SELECTIVITY.HALF_RANGE
            isConsecutive = false
            endValues.push(val)
            endOperator = 'primaryLt'
            if (startValues.length > 0) startOperator = 'primaryGte'
            isBounded = true
          }
          else if ('primaryOr' in cond || 'or' in cond) {
            selectivity *= SELECTIVITY.OR
            isConsecutive = false
          }
          else if ('like' in cond) {
            selectivity *= SELECTIVITY.LIKE
            isConsecutive = false
          }
          else {
            selectivity *= SELECTIVITY.UNKNOWN
            isConsecutive = false
          }

          if (!isBounded && field !== primaryField) {
            compositeVerifyFields.push(field)
          }
        }
      } else {
        if (field !== primaryField) {
          compositeVerifyFields.push(field)
        }
      }
    }

    if (coveredFields.length === 1 && config.fields.length === 1) {
      Object.assign(builtCondition, query[primaryField as keyof typeof query])
    }
    else {
      if (startOperator && startValues.length > 0) {
        builtCondition[startOperator] = { v: startValues.length === 1 ? startValues[0] : startValues }
      }
      if (endOperator && endValues.length > 0) {
        if (startOperator && startValues.length === endValues.length && startValues.every((val: any, i: any) => val === endValues[i])) {
          delete builtCondition[startOperator]
          builtCondition['primaryEqual'] = { v: startValues.length === 1 ? startValues[0] : startValues }
        }
        else {
          builtCondition[endOperator] = { v: endValues.length === 1 ? endValues[0] : endValues }
        }
      }
      if (Object.keys(builtCondition).length === 0) {
        Object.assign(builtCondition, query[primaryField as keyof typeof query] || {})
      }
    }

    let isIndexOrderSupported = false
    if (orderByField) {
      for (let i = 0, len = config.fields.length; i < len; i++) {
        const field = config.fields[i]
        if (field === orderByField) {
          isIndexOrderSupported = true
          break
        }
        const cond = query[field as keyof typeof query] as any
        let isExactMatch = false
        if (cond !== undefined) {
          if (typeof cond !== 'object' || cond === null) isExactMatch = true
          else if ('primaryEqual' in cond || 'equal' in cond) isExactMatch = true
        }
        if (!isExactMatch) break
      }
    }

    return {
      tree: treeTx,
      condition: builtCondition as any,
      field: primaryField,
      indexName,
      isFtsMatch: false,
      selectivity,
      compositeVerifyFields,
      coveredFields,
      isIndexOrderSupported
    } as const
  }

  /**
   * FTS 타입 인덱스의 선택도를 평가합니다.
   * FTSTermCount 통계가 있으면 실측 데이터 기반으로 선택도를 산출합니다.
   */
  evaluateFTSCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    indexName: string,
    config: any,
    query: Partial<DocumentDataplyQuery<V>>,
    queryFields: Set<string>,
    treeTx: BPTreeAsync<string | number, V>
  ) {
    const field = config.fields
    if (!queryFields.has(field)) return null

    const condition = query[field as keyof typeof query] as Partial<DocumentDataplyCondition<U>>
    if (!condition || typeof condition !== 'object' || !('match' in condition)) return null

    const ftsConfig = this.api.indexManager.getFtsConfig(config as any)
    const matchTokens = ftsConfig ? tokenize((condition as any).match as string, ftsConfig) : []

    let selectivity: number = SELECTIVITY.FTS_DEFAULT

    const termCountProvider = this.api.analysisManager
      .getProvider<FTSTermCount<T>>('fts_term_count')

    if (termCountProvider && termCountProvider.hasSampleData && ftsConfig && matchTokens.length > 0) {
      const strategy = ftsConfig.tokenizer === 'ngram'
        ? `${ftsConfig.gramSize}gram`
        : ftsConfig.tokenizer

      const minCount = termCountProvider.getMinTokenCount(field, strategy, matchTokens)
      if (minCount >= 0) {
        const sampleSize = termCountProvider.getSampleSize()
        selectivity = Math.min(minCount / sampleSize, 1)
      }
    }

    return {
      tree: treeTx,
      condition: condition as any,
      field,
      indexName,
      isFtsMatch: true,
      matchTokens,
      selectivity,
      compositeVerifyFields: [],
      coveredFields: [field],
      isIndexOrderSupported: false
    }
  }

  /**
   * 비용 계산: effectiveScanCost + sortPenalty
   * - effectiveScanCost: 인덱스 순서 지원 + limit 존재 시 조기 종료 이점 반영
   * - sortPenalty: 인메모리 정렬의 절대 문서 수 기반 비용
   */
  private calculateCost(
    selectivity: number,
    isIndexOrderSupported: boolean,
    orderByField: string | undefined,
    N: number,
    topK: number
  ): number {
    // 실질 스캔 비용: 인덱스 순서 지원 + limit 존재 시 조기 종료
    const effectiveScanCost =
      (isIndexOrderSupported && isFinite(topK) && N > 0)
        ? Math.min(topK / N, selectivity)
        : selectivity

    // 인메모리 정렬 비용: 절대 문서 수 반영
    const estimatedSortDocs = selectivity * N
    const sortPenalty = (orderByField && !isIndexOrderSupported)
      ? Math.min(estimatedSortDocs / SELECTIVITY.SORT_THRESHOLD, 1) * SELECTIVITY.SORT_PENALTY
      : 0

    return effectiveScanCost + sortPenalty
  }

  /**
   * 실행할 최적의 인덱스를 선택합니다. (비용 기반 최적 드라이버 선택)
   * cost = selectivity + sortPenalty (낮을수록 좋음)
   */
  async getSelectivityCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    query: Partial<DocumentDataplyQuery<V>>,
    orderByField?: string,
    limit: number = Infinity,
    offset: number = 0
  ): Promise<{
    driver: ({
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: false,
      isIndexOrderSupported: boolean
    } | {
      tree: BPTreeAsync<string, V>
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: true,
      matchTokens: string[],
      isIndexOrderSupported: boolean
    }),
    others: ({
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: false,
      isIndexOrderSupported: boolean
    } | {
      tree: BPTreeAsync<string, V>
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: true,
      matchTokens: string[],
      isIndexOrderSupported: boolean
    })[],
    compositeVerifyConditions: {
      field: string,
      condition: any
    }[],
    rollback: () => void
  } | null> {
    const queryFields = new Set(Object.keys(query))
    const candidates: {
      tree: BPTreeAsync<string | number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: boolean,
      matchTokens?: string[],
      selectivity: number,
      cost: number,
      compositeVerifyFields: string[],
      coveredFields: string[],
      isIndexOrderSupported: boolean
    }[] = []

    // 전체 문서 수: 인메모리 정렬 비용과 limit 조기 종료 계산에 사용
    const metadata = await this.api.getMetadata()
    const N = metadata.rowCount
    const topK = isFinite(limit) ? offset + limit : Infinity

    for (const [indexName, config] of this.api.indexManager.registeredIndices) {
      const tree = this.api.trees.get(indexName)
      if (!tree) continue

      if (config.type === 'btree') {
        const treeTx = await tree.createTransaction()
        const candidate = this.evaluateBTreeCandidate(
          indexName,
          config as any,
          query,
          queryFields,
          treeTx as unknown as BPTreeAsync<string | number, V>,
          orderByField
        )
        if (candidate) {
          candidates.push({
            ...candidate,
            cost: this.calculateCost(candidate.selectivity, candidate.isIndexOrderSupported, orderByField, N, topK)
          } as any)
        }
      }
      else if (config.type === 'fts') {
        const treeTx = await tree.createTransaction()
        const candidate = this.evaluateFTSCandidate(
          indexName,
          config as any,
          query,
          queryFields,
          treeTx as unknown as BPTreeAsync<string | number, V>
        )
        if (candidate) {
          candidates.push({
            ...candidate,
            cost: this.calculateCost(candidate.selectivity, candidate.isIndexOrderSupported, orderByField, N, topK)
          } as any)
        }
      }
    }

    const rollback = () => {
      for (const { tree } of candidates) {
        tree.rollback()
      }
    }

    if (candidates.length === 0) {
      rollback()
      return null
    }

    // 비용 오름차순 정렬 (낮을수록 좋음)
    candidates.sort((a, b) => {
      if (a.cost !== b.cost) return a.cost - b.cost

      const aConfig = this.api.indexManager.registeredIndices.get(a.indexName)
      const bConfig = this.api.indexManager.registeredIndices.get(b.indexName)
      const aFieldCount = aConfig ? (Array.isArray(aConfig.fields) ? aConfig.fields.length : 1) : 0
      const bFieldCount = bConfig ? (Array.isArray(bConfig.fields) ? bConfig.fields.length : 1) : 0

      return aFieldCount - bFieldCount
    })

    const driver = candidates[0]
    const driverCoveredFields = new Set(driver.coveredFields)
    const nonDriverCandidates = candidates.slice(1).filter(c => !driverCoveredFields.has(c.field))

    // coveredFields가 다른 낮은 비용 후보의 부분집합인 후보 제거
    // (candidates는 이미 cost 오름차순 정렬됨)
    const others: typeof nonDriverCandidates = []
    for (let i = 0, len = nonDriverCandidates.length; i < len; i++) {
      const candidate = nonDriverCandidates[i]
      let isSubset = false
      for (let j = 0, oLen = others.length; j < oLen; j++) {
        const better = others[j]
        if (candidate.coveredFields.every(f => better.coveredFields.includes(f))) {
          isSubset = true
          break
        }
      }
      if (!isSubset) others.push(candidate)
    }

    const compositeVerifyConditions: { field: string, condition: any }[] = []
    for (let i = 0, len = driver.compositeVerifyFields.length; i < len; i++) {
      const field = driver.compositeVerifyFields[i]
      if (query[field]) {
        compositeVerifyConditions.push({ field, condition: query[field] })
      }
    }

    return {
      driver: driver as any,
      others: others as any,
      compositeVerifyConditions,
      rollback,
    }
  }
}
