import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'tmp_index_recovery')
const DB_PATH = path.join(TEST_DIR, 'test.ply')

type TestDoc = {
  name: string
  age: number
}

describe('Index Recovery - Migration Scenario', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  test('Index created inside migration should persist after restart', async () => {
    // 1. Create DB, init, then create index inside migration
    let db = DocumentDataply.Define<TestDoc>().Options({}).Open(DB_PATH)
    await db.init()
    await db.migration(1, async (tx) => {
      await db.createIndex('idx_name', { type: 'btree', fields: ['name'] }, tx)
    })
    await db.insert({ name: 'Alice', age: 30 })

    const metaBefore = await db.getMetadata()
    console.log('[Before close] metadata.indices:', metaBefore.indices)

    await db.close()

    // 2. Reopen DB WITHOUT calling createIndex or migration
    db = DocumentDataply.Define<TestDoc>().Options({}).Open(DB_PATH)
    await db.init()

    const metaAfter = await db.getMetadata()
    console.log('[After reopen] metadata.indices:', metaAfter.indices)

    // Verify index is restored
    expect(metaAfter.indices).toContain('idx_name')

    // Verify query works
    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')

    await db.close()
  })

  test('Index created inside migration with multiple indices', async () => {
    // 1. Create DB with migration that creates 2 indices
    let db = DocumentDataply.Define<TestDoc>().Options({}).Open(DB_PATH)
    await db.init()
    await db.migration(1, async (tx) => {
      await db.createIndex('idx_name', { type: 'btree', fields: ['name'] }, tx)
      await db.createIndex('idx_age', { type: 'btree', fields: ['age'] }, tx)
    })
    await db.insert({ name: 'Alice', age: 30 })
    await db.close()

    // 2. Reopen
    db = DocumentDataply.Define<TestDoc>().Options({}).Open(DB_PATH)
    await db.init()

    const meta = await db.getMetadata()
    console.log('[After reopen] metadata.indices:', meta.indices)
    expect(meta.indices).toContain('idx_name')
    expect(meta.indices).toContain('idx_age')

    const nameResults = await db.select({ name: 'Alice' }).drain()
    expect(nameResults).toHaveLength(1)

    await db.close()
  })
})
