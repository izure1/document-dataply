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
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })

    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')

    // Age has no index, so querying by age should throw error (current limitation/design)
    expect(() => db.select({ age: 30 } as any)).toThrow()

    await db.close()
  })

  test('Should backfill index when option is enabled later', async () => {
    // 1. Start without index (name: false means index new inserts but don't backfill)
    // For this test, we DON'T include name in indecies at all initially
    let db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        // name not included - no tree created
      }
    }).Open(DB_PATH)
    await db.init()
    const pk1 = await db.insert({ name: 'Charlie', age: 40 })
    const pk2 = await db.insert({ name: 'David', age: 45 })

    // Confirm no index usage - name tree doesn't exist
    expect(() => db.select({ name: 'Charlie' })).toThrow()
    await db.close()

    // 2. Restart with index enabled (name: true) -> Backfill should trigger
    db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)
    await db.init()

    // Now query should work because backfill happened
    let results = await db.select({ name: 'Charlie' }).drain()
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
    let db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)
    await db.init()
    await db.insert({ name: 'Frank', age: 50 })

    // Query should work
    let results = await db.select({ name: 'Frank' }).drain()
    expect(results).toHaveLength(1)
    await db.close()

    // 2. Restart with name: false
    // According to readme: false means "don't backfill old data, but index new inserts"
    // Since data was already indexed before, query should still work
    db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: false
      }
    }).Open(DB_PATH)
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
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        // name not included at all
      }
    }).Open(DB_PATH)
    await db.init()
    await db.insert({ name: 'Henry', age: 60 })

    // Name tree doesn't exist, so query returns empty
    expect(() => db.select({ name: 'Henry' })).toThrow()

    await db.close()
  })

  test('Should delete a single document by query', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })
    await db.insert({ name: 'Charlie', age: 35 })

    // Delete Alice
    const deletedCount = await db.delete({ name: 'Alice' })
    expect(deletedCount).toBe(1)

    // Verify Alice is deleted
    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(0)

    // Verify others still exist
    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults).toHaveLength(1)
    expect(bobResults[0].name).toBe('Bob')

    const charlieResults = await db.select({ name: 'Charlie' }).drain()
    expect(charlieResults).toHaveLength(1)

    await db.close()
  })

  test('Should delete multiple documents matching query', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Alice', age: 40 }) // Same name, different age
    await db.insert({ name: 'Bob', age: 25 })

    // Delete all Alice documents
    const deletedCount = await db.delete({ name: 'Alice' })
    expect(deletedCount).toBe(2)

    // Verify all Alice are deleted
    const aliceResults = await db.select({ name: 'Alice' }).drain()
    expect(aliceResults).toHaveLength(0)

    // Verify Bob still exists
    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults).toHaveLength(1)

    await db.close()
  })

  test('Should return 0 when no documents match delete query', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })

    // Try to delete non-existent document
    const deletedCount = await db.delete({ name: 'NonExistent' })
    expect(deletedCount).toBe(0)

    // Verify Alice is untouched
    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(1)

    await db.close()
  })

  test('Should persist delete after database restart', async () => {
    let db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })

    // Delete Alice
    await db.delete({ name: 'Alice' })
    await db.close()

    // Reopen database
    db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)
    await db.init()

    // Verify Alice is still deleted
    const aliceResults = await db.select({ name: 'Alice' }).drain()
    expect(aliceResults).toHaveLength(0)

    // Verify Bob still exists
    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults).toHaveLength(1)

    await db.close()
  })

  test('Should partial update document with object', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })

    // Update Alice's age
    const updatedCount = await db.partialUpdate({ name: 'Alice' }, { age: 35 })
    expect(updatedCount).toBe(1)

    // Verify update
    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')
    expect(results[0].age).toBe(35)

    // Verify Bob is untouched
    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults[0].age).toBe(25)

    await db.close()
  })

  test('Should partial update document with function', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })

    // Update all documents' age by adding 10
    const updatedCount = await db.partialUpdate({}, (doc) => ({ age: doc.age + 10 }))
    expect(updatedCount).toBe(2)

    // Verify updates
    const aliceResults = await db.select({ name: 'Alice' }).drain()
    expect(aliceResults[0].age).toBe(40)

    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults[0].age).toBe(35)

    await db.close()
  })

  test('Should update indexed field and reindex', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })

    // Update indexed field (name)
    const updatedCount = await db.partialUpdate({ name: 'Alice' }, { name: 'Alicia' })
    expect(updatedCount).toBe(1)

    // Old name should not find any results
    const oldResults = await db.select({ name: 'Alice' }).drain()
    expect(oldResults).toHaveLength(0)

    // New name should find the document
    const newResults = await db.select({ name: 'Alicia' }).drain()
    expect(newResults).toHaveLength(1)
    expect(newResults[0].name).toBe('Alicia')
    expect(newResults[0].age).toBe(30)

    await db.close()
  })

  test('Should persist partial update after database restart', async () => {
    let db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })

    // Update
    await db.partialUpdate({ name: 'Alice' }, { age: 50, name: 'AliceUpdated' })
    await db.close()

    // Reopen database
    db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)
    await db.init()

    // Verify old name not found
    const oldResults = await db.select({ name: 'Alice' }).drain()
    expect(oldResults).toHaveLength(0)

    // Verify new name found with updated age
    const newResults = await db.select({ name: 'AliceUpdated' }).drain()
    expect(newResults).toHaveLength(1)
    expect(newResults[0].age).toBe(50)

    await db.close()
  })

  test('Should full update document with object', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })
    await db.insert({ name: 'Bob', age: 25 })

    // Full update Alice - completely replace document
    const updatedCount = await db.fullUpdate({ name: 'Alice' }, { name: 'Alicia', age: 100 })
    expect(updatedCount).toBe(1)

    // Verify old name not found
    const oldResults = await db.select({ name: 'Alice' }).drain()
    expect(oldResults).toHaveLength(0)

    // Verify new document
    const newResults = await db.select({ name: 'Alicia' }).drain()
    expect(newResults).toHaveLength(1)
    expect(newResults[0].name).toBe('Alicia')
    expect(newResults[0].age).toBe(100)

    // Verify Bob is untouched
    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults).toHaveLength(1)
    expect(bobResults[0].age).toBe(25)

    await db.close()
  })

  test('Should full update document with function', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    await db.insert({ name: 'Alice', age: 30 })

    // Full update using function
    const updatedCount = await db.fullUpdate(
      { name: 'Alice' },
      (doc) => ({ name: doc.name + '_updated', age: doc.age * 2 })
    )
    expect(updatedCount).toBe(1)

    // Verify old name not found
    const oldResults = await db.select({ name: 'Alice' }).drain()
    expect(oldResults).toHaveLength(0)

    // Verify new document
    const newResults = await db.select({ name: 'Alice_updated' }).drain()
    expect(newResults).toHaveLength(1)
    expect(newResults[0].age).toBe(60)

    await db.close()
  })

  test('Should preserve _id on full update', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        name: true
      }
    }).Open(DB_PATH)

    await db.init()
    const originalId = await db.insert({ name: 'Alice', age: 30 })

    // Full update
    await db.fullUpdate({ _id: originalId } as any, { name: 'Alicia', age: 100 })

    // Verify _id is preserved
    const results = await db.select({ _id: originalId } as any).drain()
    expect(results).toHaveLength(1)
    expect(results[0]._id).toBe(originalId)
    expect(results[0].name).toBe('Alicia')

    await db.close()
  })
})
