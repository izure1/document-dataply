import { DocumentDataply } from '../src/core'
import { DataplyDocument } from '../src/types'
import * as fs from 'node:fs'
import * as path from 'node:path'

type TestDoc = {
  name: string
  age?: number
  role?: string
}

describe('DocumentDataply Upsert', () => {
  const dbPath = path.join(__dirname, 'test_upsert.db')
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

  test('should insert new document when _id is not provided', async () => {
    const docToUpsert = { name: 'Alice', age: 30 } as DataplyDocument<TestDoc>
    const updatedCount = await db.upsert(docToUpsert)

    expect(updatedCount).toBe(0)

    const results = await db.select({ name: 'Alice' }).drain()
    expect(results.length).toBe(1)
    expect(results[0].name).toBe('Alice')
    expect(results[0].age).toBe(30)
    expect(results[0]._id).toBeDefined()
  })

  test('should ignore non-existing _id and insert as new document with auto-generated _id', async () => {
    const docWithFakeId = { _id: 9999, name: 'Bob', age: 25 } as DataplyDocument<TestDoc>
    const updatedCount = await db.upsert(docWithFakeId)

    expect(updatedCount).toBe(0)

    // _id 9999로 조회하면 나오지 않아야함
    const fakeIdResults = await db.select({ _id: 9999 }).drain()
    expect(fakeIdResults.length).toBe(0)

    // name: 'Bob'으로 조회하면 새 _id(예: 1)를 가진 문서가 나와야함
    const bobResults = await db.select({ name: 'Bob' }).drain()
    expect(bobResults.length).toBe(1)
    expect(bobResults[0]._id).not.toBe(9999)
    expect(bobResults[0].name).toBe('Bob')
    expect(bobResults[0].age).toBe(25)
  })

  test('should fullUpdate existing document when _id exists in DB', async () => {
    // 1. 초기 문서 삽입
    const initialId = await db.insert({ name: 'Charlie', age: 40, role: 'Developer' })
    expect(initialId).toBeGreaterThan(0)

    // 2. 해당 _id를 가지고 fullUpdate upsert 실행 (role 필드가 제외된 fullUpdate)
    const docToUpdate: DataplyDocument<TestDoc> = {
      _id: initialId,
      name: 'Charlie Updated',
      age: 41
    }
    const updatedCount = await db.upsert(docToUpdate)

    expect(updatedCount).toBe(1)

    // 3. 검증: 문수가 1개여야 하고, fullUpdate 되었으므로 role 필드는 삭제(undefined)되어야 함
    const results = await db.select({ _id: initialId }).drain()
    expect(results.length).toBe(1)
    expect(results[0]._id).toBe(initialId)
    expect(results[0].name).toBe('Charlie Updated')
    expect(results[0].age).toBe(41)
    expect(results[0].role).toBeUndefined()
  })

  test('should work correctly inside write transaction', async () => {
    let upsertResult1 = -1
    let upsertResult2 = -1

    await db.withWriteTransaction(async (tx) => {
      // 1. 신규 삽입
      const doc1 = { name: 'David', age: 50 } as DataplyDocument<TestDoc>
      upsertResult1 = await db.upsert(doc1, tx)

      // 2. 조회해서 생성된 _id 파악
      const found = await db.select({ name: 'David' }, {}, tx).drain()
      const generatedId = found[0]._id

      // 3. 업데이트
      const doc2 = { _id: generatedId, name: 'David Updated', age: 51 } as DataplyDocument<TestDoc>
      upsertResult2 = await db.upsert(doc2, tx)
    })

    expect(upsertResult1).toBe(0)
    expect(upsertResult2).toBe(1)

    const finalResults = await db.select({ name: 'David Updated' }).drain()
    expect(finalResults.length).toBe(1)
    expect(finalResults[0].age).toBe(51)
  })

  test('should update index correctly on upsert fullUpdate', async () => {
    // 1. 초기 문서 삽입
    const id = await db.insert({ name: 'Eve', age: 20 })

    // 2. 이전 인덱스 값('Eve', 20)으로 조회 가능 확인
    const beforeResults = await db.select({ name: 'Eve', age: 20 }).drain()
    expect(beforeResults.length).toBe(1)

    // 3. 인덱스 대상 필드(name, age) 수정하여 upsert
    await db.upsert({ _id: id, name: 'Eve New', age: 21 })

    // 4. 이전 인덱스 값으로 조회 불가 확인
    const oldIndexResults = await db.select({ name: 'Eve' }).drain()
    expect(oldIndexResults.length).toBe(0)

    // 5. 새 인덱스 값으로 조회 가능 확인
    const newIndexResults = await db.select({ name: 'Eve New', age: 21 }).drain()
    expect(newIndexResults.length).toBe(1)
    expect(newIndexResults[0]._id).toBe(id)
  })
})
