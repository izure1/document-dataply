import { DocumentDataply } from '../src/core/document'
import * as fs from 'fs'
import * as path from 'path'

type DataDoc = {
  score: number
  category: string
  active: boolean
}

describe('DocumentDataply Query Operators', () => {
  const dbPath = path.join(__dirname, 'test_query.db')
  let db: DocumentDataply<DataDoc>

  // Helper to initialize data for each test
  async function initData() {
    await db.insert({ score: 10, category: 'A', active: true })
    await db.insert({ score: 20, category: 'B', active: false })
    await db.insert({ score: 30, category: 'A', active: true })
    await db.insert({ score: 40, category: 'C', active: false })
    await db.insert({ score: 50, category: 'B', active: true })
  }

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = new DocumentDataply(dbPath, {
      indices: {
        score: true,
        category: true,
        active: true,
      }
    })
    await db.init()
    await initData()
  })

  afterEach(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
  })

  test('should support lt operator', async () => {
    const results = await db.select({ score: { lt: 30 } }).drain()
    expect(results.length).toBe(2)
    results.forEach(r => expect(r.score).toBeLessThan(30))
  })

  test('should support lte operator', async () => {
    const results = await db.select({ score: { lte: 30 } }).drain()
    expect(results.length).toBe(3)
    results.forEach(r => expect(r.score).toBeLessThanOrEqual(30))
  })

  test('should support gt operator', async () => {
    const results = await db.select({ score: { gt: 30 } }).drain()
    expect(results.length).toBe(2)
    results.forEach(r => expect(r.score).toBeGreaterThan(30))
  })

  test('should support gte operator', async () => {
    const results = await db.select({ score: { gte: 30 } }).drain()
    expect(results.length).toBe(3)
    results.forEach(r => expect(r.score).toBeGreaterThanOrEqual(30))
  })

  test('should support notEqual operator', async () => {
    const results = await db.select({ category: { notEqual: 'A' } }).drain()
    expect(results.length).toBe(3)
    results.forEach(r => expect(r.category).not.toBe('A'))
  })

  test('should support like operator with exact match', async () => {
    await db.insert({ score: 60, category: 'Apple', active: true })
    const results = await db.select({ category: { like: 'Apple' } }).drain()
    expect(results.length).toBe(1)
    expect(results[0].category).toBe('Apple')
  })

  test('should support like operator with % wildcard', async () => {
    await db.insert({ score: 60, category: 'Apple', active: true })
    // 'Apple' matches 'App%'
    const results = await db.select({ category: { like: 'App%' } }).drain()
    expect(results.length).toBe(1)
    expect(results[0].category).toBe('Apple')
  })

  test('should support like operator with _ wildcard', async () => {
    // 'A', 'B', 'A', 'C', 'B' match '_'
    // 'Apple' (if inserted) does not match '_'
    const results = await db.select({ category: { like: '_' } }).drain()
    expect(results.length).toBe(5)
  })

  test('should support or operator', async () => {
    const results = await db.select({
      category: { or: ['A', 'C'] }
    }).drain()
    // category A(2개) + C(1개) = 3개
    expect(results.length).toBe(3)
    results.forEach(r => expect(['A', 'C']).toContain(r.category))
  })

  test('should support multiple conditions (AND)', async () => {
    const results = await db.select({
      category: 'A',
      score: { gt: 15 }
    }).drain()
    expect(results.length).toBe(1)
    expect(results[0].score).toBe(30)
  })

  test('should respect limit', async () => {
    const results = await db.select({ active: true }, { limit: 1 }).drain()
    expect(results.length).toBe(1)
  })
})
