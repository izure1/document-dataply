import { DocumentDataply } from '../src/core/document'
import * as fs from 'fs'
import * as path from 'path'

const TEST_DIR = path.join(__dirname, 'tmp_doc_index')
const DB_PATH = path.join(TEST_DIR, 'test.ply')

type TestDoc = {
  name: string
  age: number
  tags?: string[]
}

describe('Document Indexing Options', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  afterEach(async () => {
    // Clean up
  })

  test('Should insert and query with enabled index', async () => {
    const db = new DocumentDataply<TestDoc>(DB_PATH, {
      indices: {
        name: true
      }
    })
    await db.init()

    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })

    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')

    // Age has no index, so querying by age should return empty (current limitation/design)
    const ageResults = await db.select({ age: 30 }).drain()
    expect(ageResults).toHaveLength(0)

    await db.close()
  })

  test('Should backfill index when option is enabled later', async () => {
    // 1. Start without index (name: false means index new inserts but don't backfill)
    // For this test, we DON'T include name in indecies at all initially
    let db = new DocumentDataply<TestDoc>(DB_PATH, {
      indices: {
        // name not included - no tree created
      }
    })
    await db.init()
    const pk1 = await db.insert({ name: 'Charlie', age: 40 })
    const pk2 = await db.insert({ name: 'David', age: 45 })

    // Confirm no index usage - name tree doesn't exist
    let results = await db.select({ name: 'Charlie' }).drain()
    expect(results).toHaveLength(0)
    await db.close()

    // 2. Restart with index enabled (name: true) -> Backfill should trigger
    db = new DocumentDataply<TestDoc>(DB_PATH, {
      indices: {
        name: true
      }
    })
    await db.init()

    // Now query should work because backfill happened
    results = await db.select({ name: 'Charlie' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Charlie')

    // Insert new data
    await db.insert({ name: 'Eve', age: 20 })
    results = await db.select({ name: 'Eve' }).drain()
    expect(results).toHaveLength(1)

    await db.close()
  })

  test('Should index new inserts with false option but not backfill old data', async () => {
    // 1. Start with name: true, insert data
    let db = new DocumentDataply<TestDoc>(DB_PATH, {
      indices: {
        name: true
      }
    })
    await db.init()
    await db.insert({ name: 'Frank', age: 50 })

    // Query should work
    let results = await db.select({ name: 'Frank' }).drain()
    expect(results).toHaveLength(1)
    await db.close()

    // 2. Restart with name: false
    // According to readme: false means "don't backfill old data, but index new inserts"
    // Since data was already indexed before, query should still work
    db = new DocumentDataply<TestDoc>(DB_PATH, {
      indices: {
        name: false
      }
    })
    await db.init()

    // Previously indexed data should still be queryable (tree is loaded)
    results = await db.select({ name: 'Frank' }).drain()
    expect(results).toHaveLength(1)

    // New insert should also be indexed
    await db.insert({ name: 'Grace', age: 55 })
    results = await db.select({ name: 'Grace' }).drain()
    expect(results).toHaveLength(1)

    await db.close()
  })

  test('Should not create index tree when field is not in indecies', async () => {
    const db = new DocumentDataply<TestDoc>(DB_PATH, {
      indices: {
        // name not included at all
      }
    })
    await db.init()
    await db.insert({ name: 'Henry', age: 60 })

    // Name tree doesn't exist, so query returns empty
    const results = await db.select({ name: 'Henry' }).drain()
    expect(results).toHaveLength(0)

    await db.close()
  })
})
