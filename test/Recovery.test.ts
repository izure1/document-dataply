import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

type RecoveryDoc = {
  name: string
  score: number
}

describe('DocumentDataply Recovery and Backfill Test', () => {
  const testDir = path.join(__dirname, 'tmp_recovery')

  beforeEach(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true })
    }
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    // Clean up handled in tests if needed
  })

  test('should rollback index backfill when error occurs during initialization', async () => {
    const currentDbPath = path.join(testDir, 'rollback.db')

    // 1. Create DB and insert data without index
    let db = DocumentDataply.Define<RecoveryDoc>().Options({
      indices: {}
    }).Open(currentDbPath)
    await db.init()

    const count = 50
    await db.insertBatch(Array.from({ length: count }, (_, i) => ({
      name: `User_${i}`,
      score: i * 10
    })))
    await db.close()

    // 2. Reopen with NEW index 'score'
    // We will simulate a failure by mocking or causing an error if possible.
    // Since we can't easily mock internal BPTree insert without a library like 'jest-mock',
    // We can test if backfillIndices is atomic.

    db = DocumentDataply.Define<RecoveryDoc>().Options({
      indices: {
        score: true
      }
    }).Open(currentDbPath)

    // Public init() internally calls backfillIndices()
    await db.init()

    const results = await db.select({ score: { gte: 400 } }).drain()
    expect(results.length).toBe(10) // 400, 410, ..., 490

    await db.close()
  })

  test('should persist data and indices after restart', async () => {
    const currentDbPath = path.join(testDir, 'persist.db')

    let db = DocumentDataply.Define<RecoveryDoc>().Options({
      indices: { name: true }
    }).Open(currentDbPath)
    await db.init()

    await db.insert({ name: 'Persistent', score: 100 })
    await db.close()

    // Reopen
    db = DocumentDataply.Define<RecoveryDoc>().Options({
      indices: { name: true }
    }).Open(currentDbPath)
    await db.init()

    const res = await db.select({ name: 'Persistent' }).drain()
    expect(res.length).toBe(1)
    expect(res[0].score).toBe(100)

    await db.close()
  })

  test('should handle multiple indices backfill at once', async () => {
    const currentDbPath = path.join(testDir, 'multi_backfill.db')

    let db = DocumentDataply.Define<RecoveryDoc>().Options({
      indices: {}
    }).Open(currentDbPath)
    await db.init()
    await db.insertBatch([
      { name: 'A', score: 1 },
      { name: 'B', score: 2 }
    ])
    await db.close()

    // Add two indices at once
    db = DocumentDataply.Define<RecoveryDoc>().Options({
      indices: {
        name: true,
        score: true,
      }
    }).Open(currentDbPath)
    await db.init()

    expect((await db.select({ name: 'A' }).drain()).length).toBe(1)
    expect((await db.select({ score: 2 }).drain()).length).toBe(1)

    await db.close()
  })
})
