import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'tmp_migration_index')
const DB_PATH = path.join(TEST_DIR, 'migration_test.ply')

type TestDoc = {
  name: string
  age: number
}

describe('Migration and Indexing Flow', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  afterEach(async () => {
    // Clean up if needed
  })

  test('Should create index within migration and insert data after', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({ logLevel: 0 }).Open(DB_PATH)

    // 1. Initialize DB
    await db.init()

    // 2. Run Migration to create index
    await db.migration(1, async (tx) => {
      await db.createIndex('idx_name', { type: 'btree', fields: ['name'] }, tx)
    })

    // 3. Insert data after migration
    await db.insert({ name: 'MigrationUser', age: 30 })

    // 4. Verify indexing works
    const results = await db.select({ name: 'MigrationUser' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('MigrationUser')
    expect(results[0].age).toBe(30)

    // 5. Verify index exists in metadata
    const metadata = await db.getMetadata()
    expect(metadata.indices).toContain('idx_name')

    await db.close()
  })

  test('Should handle multiple migrations and index persistence', async () => {
    let db = DocumentDataply.Define<TestDoc>().Options({ logLevel: 0 }).Open(DB_PATH)
    await db.init()

    // Migration V1: Create name index
    await db.migration(1, async (tx) => {
      await db.createIndex('idx_name', { type: 'btree', fields: ['name'] }, tx)
    })

    await db.insert({ name: 'User1', age: 20 })
    await db.close()

    // Reopen: register indexes before init, then run migration
    db = DocumentDataply.Define<TestDoc>().Options({ logLevel: 0 }).Open(DB_PATH)
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db.init()

    // Migration V2: Create age index (V1 callback will be skipped since version >= 1)
    await db.migration(2, async (tx) => {
      await db.createIndex('idx_age', { type: 'btree', fields: ['age'] }, tx)
    })

    await db.insert({ name: 'User2', age: 40 })

    // Verify both indexes
    const nameResults = await db.select({ name: 'User1' }).drain()
    expect(nameResults).toHaveLength(1)

    const ageResults = await db.select({ age: 40 }).drain()
    expect(ageResults).toHaveLength(1)
    expect(ageResults[0].name).toBe('User2')

    await db.close()
  })
})
