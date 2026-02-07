import { DocumentDataply } from '../src/core/document'
import * as fs from 'node:fs'
import * as path from 'node:path'

describe('Top-K Optimization', () => {
  const dbPath = path.join(__dirname, 'tmp_topk')
  let db: DocumentDataply<any, any>

  beforeAll(async () => {
    if (fs.existsSync(dbPath)) fs.rmSync(dbPath, { recursive: true, force: true })
    db = DocumentDataply.Define<{ name: string; age: number }>()
      .Options({
        indices: {
          age: true // age IS indexed to satisfy the check
        }
      })
      .Open(dbPath)
    await db.init()

    // Insert 1000 items
    const docs = []
    for (let i = 0; i < 1000; i++) {
      docs.push({ name: `user${i}`, age: i })
    }
    // Shuffle docs
    for (let i = docs.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [docs[i], docs[j]] = [docs[j], docs[i]]
    }
    await db.insertBatch(docs)
  })

  afterAll(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) fs.rmSync(dbPath, { recursive: true, force: true })
  })

  it('should correctly return top-k items in ascending order', async () => {
    const { drain } = db.select({}, { orderBy: 'age', sortOrder: 'asc', limit: 10 })
    const results = await drain()
    expect(results.length).toBe(10)
    expect(results[0].age).toBe(0)
    expect(results[9].age).toBe(9)
    for (let i = 0; i < 9; i++) {
      expect(results[i].age).toBeLessThan(results[i + 1].age)
    }
  })

  it('should correctly return top-k items in descending order', async () => {
    const { drain } = db.select({}, { orderBy: 'age', sortOrder: 'desc', limit: 10 })
    const results = await drain()
    expect(results.length).toBe(10)
    expect(results[0].age).toBe(999)
    expect(results[9].age).toBe(990)
    for (let i = 0; i < 9; i++) {
      expect(results[i].age).toBeGreaterThan(results[i + 1].age)
    }
  })

  it('should correctly handle offset', async () => {
    const { drain } = db.select({}, { orderBy: 'age', sortOrder: 'asc', limit: 10, offset: 20 })
    const results = await drain()
    expect(results.length).toBe(10)
    expect(results[0].age).toBe(20)
    expect(results[9].age).toBe(29)
  })
})
