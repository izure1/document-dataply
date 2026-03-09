import type { DataplyTreeValue, DocumentDataplyQuery, DocumentDataplyCondition } from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import type { FTSTermCount } from './analysis/FTSTermCount'
import { BPTreeAsync } from 'dataply'
import { tokenize } from '../utils/tokenizer'

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
    let score = 0
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
      score += 1

      if (isConsecutive) {
        const cond = query[field as keyof typeof query] as any
        if (cond !== undefined) {
          let isBounded = false

          if (typeof cond !== 'object' || cond === null) {
            score += 100
            startValues.push(cond)
            endValues.push(cond)
            startOperator = 'primaryGte'
            endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryEqual' in cond || 'equal' in cond) {
            const val = cond.primaryEqual?.v ?? cond.equal?.v ?? cond.primaryEqual ?? cond.equal
            score += 100
            startValues.push(val)
            endValues.push(val)
            startOperator = 'primaryGte'
            endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryGte' in cond || 'gte' in cond) {
            const val = cond.primaryGte?.v ?? cond.gte?.v ?? cond.primaryGte ?? cond.gte
            score += 50
            isConsecutive = false
            startValues.push(val)
            startOperator = 'primaryGte'
            if (endValues.length > 0) endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryGt' in cond || 'gt' in cond) {
            const val = cond.primaryGt?.v ?? cond.gt?.v ?? cond.primaryGt ?? cond.gt
            score += 50
            isConsecutive = false
            startValues.push(val)
            startOperator = 'primaryGt'
            if (endValues.length > 0) endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryLte' in cond || 'lte' in cond) {
            const val = cond.primaryLte?.v ?? cond.lte?.v ?? cond.primaryLte ?? cond.lte
            score += 50
            isConsecutive = false
            endValues.push(val)
            endOperator = 'primaryLte'
            if (startValues.length > 0) startOperator = 'primaryGte'
            isBounded = true
          }
          else if ('primaryLt' in cond || 'lt' in cond) {
            const val = cond.primaryLt?.v ?? cond.lt?.v ?? cond.primaryLt ?? cond.lt
            score += 50
            isConsecutive = false
            endValues.push(val)
            endOperator = 'primaryLt'
            if (startValues.length > 0) startOperator = 'primaryGte'
            isBounded = true
          }
          else if ('primaryOr' in cond || 'or' in cond) {
            score += 20
            isConsecutive = false
          }
          else if ('like' in cond) {
            score += 15
            isConsecutive = false
          }
          else {
            score += 10
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
      if (isIndexOrderSupported) {
        score += 200
      }
    }

    return {
      tree: treeTx,
      condition: builtCondition as any,
      field: primaryField,
      indexName,
      isFtsMatch: false,
      score,
      compositeVerifyFields,
      coveredFields,
      isIndexOrderSupported
    } as const
  }

  /**
   * FTS 타입 인덱스의 선택도를 평가합니다.
   * FTSTermCount 통계가 있으면 토큰 빈도 기반 동적 score를 산출합니다.
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

    // 통계 기반 동적 score 산출
    // MAX_FTS_SCORE=400: 희귀 토큰이 B-Tree orderBy(+200)를 이길 수 있도록 설정
    // selectivity ~30% 이하 → FTS 우선, ~40% 이상 → orderBy 우선
    const MAX_FTS_SCORE = 400
    const MIN_FTS_SCORE = 10
    const DEFAULT_FTS_SCORE = 90

    let score = DEFAULT_FTS_SCORE

    const termCountProvider = this.api.analysisManager
      .getProvider<FTSTermCount<T>>('fts_term_count')

    if (termCountProvider && termCountProvider.hasSampleData && ftsConfig && matchTokens.length > 0) {
      const strategy = ftsConfig.tokenizer === 'ngram'
        ? `${ftsConfig.gramSize}gram`
        : ftsConfig.tokenizer

      const minCount = termCountProvider.getMinTokenCount(field, strategy, matchTokens)
      if (minCount >= 0) {
        const sampleSize = termCountProvider.getSampleSize()
        const selectivityRatio = Math.min(minCount / sampleSize, 1)
        score = Math.round(MAX_FTS_SCORE * (1 - selectivityRatio) + MIN_FTS_SCORE)
      }
    }

    return {
      tree: treeTx,
      condition: condition as any,
      field,
      indexName,
      isFtsMatch: true,
      matchTokens,
      score,
      compositeVerifyFields: [],
      coveredFields: [field],
      isIndexOrderSupported: false
    } as const
  }

  /**
   * 실행할 최적의 인덱스를 선택합니다. (최적 드라이버 선택)
   */
  async getSelectivityCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    query: Partial<DocumentDataplyQuery<V>>,
    orderByField?: string
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
      score: number,
      compositeVerifyFields: string[],
      coveredFields: string[],
      isIndexOrderSupported: boolean
    }[] = []

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
        if (candidate) candidates.push(candidate as any)
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
        if (candidate) candidates.push(candidate as any)
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

    candidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score

      const aConfig = this.api.indexManager.registeredIndices.get(a.indexName)
      const bConfig = this.api.indexManager.registeredIndices.get(b.indexName)
      const aFieldCount = aConfig ? (Array.isArray(aConfig.fields) ? aConfig.fields.length : 1) : 0
      const bFieldCount = bConfig ? (Array.isArray(bConfig.fields) ? bConfig.fields.length : 1) : 0

      return aFieldCount - bFieldCount
    })

    const driver = candidates[0]
    const driverCoveredFields = new Set(driver.coveredFields)
    const others = candidates.slice(1).filter(c => !driverCoveredFields.has(c.field))

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
