import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'tmp_full_update_removal')
const DB_PATH = path.join(TEST_DIR, 'test.ply')

type TestDoc = {
  a?: number
  b?: number
  c?: number
  d?: number
}

describe('fullUpdate Field Removal', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  afterEach(async () => {
    // Clean up
  })

  test('fullUpdate should remove fields not in new document', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        a: true
      }
    }).Open(DB_PATH)

    await db.init()

    // Insert document with { a, b, c }
    const id = await db.insert({ a: 1, b: 2, c: 3 })

    // Verify initial document
    let results = await db.select({ _id: id } as any).drain()
    expect(results).toHaveLength(1)
    expect(results[0]).toMatchObject({ _id: id, a: 1, b: 2, c: 3 })

    // fullUpdate with { a, b, d } - c should be removed, d should be added
    const updatedCount = await db.fullUpdate(
      { _id: id } as any,
      { a: 10, b: 20, d: 40 }
    )
    expect(updatedCount).toBe(1)

    // Verify updated document
    results = await db.select({ _id: id } as any).drain()
    expect(results).toHaveLength(1)

    const updatedDoc = results[0]
    console.log('Updated document:', updatedDoc)

    // c should NOT exist (removed)
    expect(updatedDoc.c).toBeUndefined()
    // d should exist (added)
    expect(updatedDoc.d).toBe(40)
    // a and b should be updated
    expect(updatedDoc.a).toBe(10)
    expect(updatedDoc.b).toBe(20)
    // _id should be preserved
    expect(updatedDoc._id).toBe(id)

    await db.close()
  })

  test('partialUpdate should preserve fields not in update', async () => {
    const db = DocumentDataply.Define<TestDoc>().Options({
      indices: {
        a: true
      }
    }).Open(DB_PATH)

    await db.init()

    // Insert document with { a, b, c }
    const id = await db.insert({ a: 1, b: 2, c: 3 })

    // partialUpdate with { a, d } - c should be preserved
    await db.partialUpdate(
      { _id: id } as any,
      { a: 10, d: 40 }
    )

    // Verify updated document
    const results = await db.select({ _id: id } as any).drain()
    const updatedDoc = results[0]

    console.log('Partial updated document:', updatedDoc)

    // c should still exist (preserved)
    expect(updatedDoc.c).toBe(3)
    // b should still exist (preserved)
    expect(updatedDoc.b).toBe(2)
    // d should exist (added)
    expect(updatedDoc.d).toBe(40)
    // a should be updated
    expect(updatedDoc.a).toBe(10)

    await db.close()
  })
})
