import { DocumentDataply } from '../src/core'
import { DataplyDocument } from '../src/types'
import * as fs from 'node:fs'
import * as path from 'node:path'

type TestDoc = {
  name: string
  age?: number
  role?: string
}

describe('DocumentDataply UpsertBatch', () => {
  const dbPath = path.join(__dirname, 'test_upsert_batch.db')
  let db: DocumentDataply<TestDoc>

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = DocumentDataply.Define<TestDoc>().Options({ logLevel: 3 }).Open(dbPath)
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db.createIndex('idx_age', { type: 'btree', fields: ['age'] })
    await db.init()
  })

  afterEach(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
  })

  test('should return 0 when input array is empty', async () => {
    const updatedCount = await db.upsertBatch([])
    expect(updatedCount).toBe(0)
  })

  test('should insert all new documents when _id is not provided', async () => {
    const docsToUpsert: (TestDoc | DataplyDocument<TestDoc>)[] = [
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 }
    ]

    const updatedCount = await db.upsertBatch(docsToUpsert)
    expect(updatedCount).toBe(0)

    const allDocs = await db.select({}).drain()
    expect(allDocs.length).toBe(3)
  })

  test('should ignore non-existing _ids and insert all as new documents', async () => {
    const docsWithFakeId: (TestDoc | DataplyDocument<TestDoc>)[] = [
      { _id: 9991, name: 'David', age: 40 },
      { _id: 9992, name: 'Eve', age: 22 }
    ]

    const updatedCount = await db.upsertBatch(docsWithFakeId)
    expect(updatedCount).toBe(0)

    const fake1 = await db.select({ _id: 9991 }).drain()
    expect(fake1.length).toBe(0)

    const david = await db.select({ name: 'David' }).drain()
    expect(david.length).toBe(1)
    expect(david[0]._id).not.toBe(9991)
  })

  test('should update existing documents and insert new ones when mixed array is provided', async () => {
    const initialPks = await db.insertBatch([
      { name: 'Alice', age: 30, role: 'User' },
      { name: 'Bob', age: 25, role: 'User' }
    ])

    const mixedDocs: (TestDoc | DataplyDocument<TestDoc>)[] = [
      { _id: initialPks[0], name: 'Alice Updated', age: 31, role: 'Admin' },
      { name: 'Charlie', age: 28, role: 'Guest' },
      { _id: 8888, name: 'FakeDoc', age: 99 },
      { _id: initialPks[1], name: 'Bob Updated', age: 26 }
    ]


    const updatedCount = await db.upsertBatch(mixedDocs)

    // initialPks[0]과 initialPks[1] 총 2개가 업데이트됨
    expect(updatedCount).toBe(2)

    // 총 문서 개수는 기존 2개 + 신규 2개 = 4개
    const allDocs = await db.select({}).drain()
    expect(allDocs.length).toBe(4)

    const alice = await db.select({ _id: initialPks[0] }).drain()
    expect(alice[0].name).toBe('Alice Updated')
    expect(alice[0].age).toBe(31)

    const charlie = await db.select({ name: 'Charlie' }).drain()
    expect(charlie.length).toBe(1)
  })

  test('should work correctly inside write transaction', async () => {
    const pks = await db.insertBatch([{ name: 'Doc1', age: 10 }])

    let updatedCount = -1
    await db.withWriteTransaction(async (tx) => {
      updatedCount = await db.upsertBatch(
        [
          { _id: pks[0], name: 'Doc1 Updated', age: 11 },
          { name: 'Doc2', age: 20 }
        ],
        tx
      )
    })

    expect(updatedCount).toBe(1)

    const doc1 = await db.select({ _id: pks[0] }).drain()
    expect(doc1[0].name).toBe('Doc1 Updated')

    const doc2 = await db.select({ name: 'Doc2' }).drain()
    expect(doc2.length).toBe(1)
  })
})
