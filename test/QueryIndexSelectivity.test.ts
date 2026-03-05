import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

describe('Composite Index Selectivity Parsing Test', () => {
  const dbPath = path.join(__dirname, 'test_composite.db')
  let db: DocumentDataply<any>
  let api: any

  beforeAll(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    db = DocumentDataply.Define<any>().Options({}).Open(dbPath)
    await db.createIndex('idx_complex', { type: 'btree', fields: ['category', 'level', 'score'] })
    await db.init()
    api = (db as any).api
  })

  afterAll(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
  })

  it('equal + gte → 복합 바운드가 배열로 생성되어야 한다', async () => {
    // verboseQuery를 통해 쿼리를 BPTree 조건 형식으로 변환
    const verboseQ = api.verboseQuery({ category: 'weapon', level: { gte: 10 } })
    const res = await api.getSelectivityCandidate(verboseQ)

    console.log('driver condition:', JSON.stringify(res?.driver?.condition))

    // 복합 인덱스 idx_complex가 선택되어야 함
    expect(res).not.toBeNull()
    expect(res.driver.indexName).toBe('idx_complex')

    // builtCondition이 배열 형태 복합 바운드를 갖는지 확인
    const cond = res.driver.condition
    expect(cond.primaryGte).toBeDefined()
    expect(cond.primaryGte.v).toEqual(['weapon', 10])

    // 상한선은 equal 필드까지만 (endValues = ['weapon'])
    expect(cond.primaryLte).toBeDefined()
    expect(cond.primaryLte.v).toBe('weapon')

    res.rollback()
  })

  it('equal + equal + gte → 3필드 복합 바운드', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: 10,
      score: { gte: 100 }
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    console.log('driver condition:', JSON.stringify(res?.driver?.condition))

    expect(res).not.toBeNull()
    expect(res.driver.indexName).toBe('idx_complex')

    const cond = res.driver.condition
    expect(cond.primaryGte).toBeDefined()
    expect(cond.primaryGte.v).toEqual(['weapon', 10, 100])

    // 상한선은 equal 2개분 ['weapon', 10]
    expect(cond.primaryLte).toBeDefined()
    expect(cond.primaryLte.v).toEqual(['weapon', 10])

    res.rollback()
  })

  it('모두 equal → primaryEqual 배열이 생성되어야 한다', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: 10,
      score: 50
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    console.log('driver condition:', JSON.stringify(res?.driver?.condition))

    expect(res).not.toBeNull()
    const cond = res.driver.condition
    expect(cond.primaryEqual).toBeDefined()
    expect(cond.primaryEqual.v).toEqual(['weapon', 10, 50])

    res.rollback()
  })

  it('gt 조건 → 하한선은 gt, 상한선은 이전 필드까지', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: { gt: 10 }
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    expect(res).not.toBeNull()
    const cond = res.driver.condition
    expect(cond.primaryGt).toBeDefined()
    expect(cond.primaryGt.v).toEqual(['weapon', 10])
    expect(cond.primaryLte).toBeDefined()
    expect(cond.primaryLte.v).toBe('weapon')

    res.rollback()
  })

  it('lt 조건 → 하한선은 이전 필드까지, 상한선은 lt', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: { lt: 10 }
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    expect(res).not.toBeNull()
    const cond = res.driver.condition
    expect(cond.primaryGte).toBeDefined()
    expect(cond.primaryGte.v).toBe('weapon')
    expect(cond.primaryLt).toBeDefined()
    expect(cond.primaryLt.v).toEqual(['weapon', 10])

    res.rollback()
  })

  it('or 조건 → 연속성 파괴 (이전 필드까지만 바운드 적용)', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: { or: [10, 20] },
      score: 100 // level에서 끊겼으므로 score 조건은 B-Tree 바운드에 포함되지 않음
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    expect(res).not.toBeNull()
    const cond = res.driver.condition

    // or 조우 시 해당 필드는 바운드에서 제외되며, 이전까지의 equal들로만 구성됨
    expect(cond.primaryEqual).toBeDefined()
    expect(cond.primaryEqual.v).toBe('weapon')

    // score 조건은 B-Tree에 타지 않았어야 함
    expect(cond.primaryGte).toBeUndefined()

    res.rollback()
  })

  it('like 조건 → 연속성 파괴', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: { like: '%10%' }
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    expect(res).not.toBeNull()
    const cond = res.driver.condition
    expect(cond.primaryEqual).toBeDefined()
    expect(cond.primaryEqual.v).toBe('weapon')

    res.rollback()
  })

  it('notEqual 및 기타 조건 → 연속성 파괴', async () => {
    const verboseQ = api.verboseQuery({
      category: 'weapon',
      level: { notEqual: { v: 10 } }
    })
    const res = await api.getSelectivityCandidate(verboseQ)

    expect(res).not.toBeNull()
    const cond = res.driver.condition
    expect(cond.primaryEqual).toBeDefined()
    expect(cond.primaryEqual.v).toBe('weapon')

    res.rollback()
  })
})
