import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'tmp_index_recovery')
const DB_PATH = path.join(TEST_DIR, 'test.ply')

type TestDoc = {
  name: string
  age: number
}

describe('Index Recovery on Restart', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  test('Should recover existing indices on restart without re-registering', async () => {
    // 1. Create DB and Index
    let db = DocumentDataply.Define<TestDoc>().Options({}).Open(DB_PATH)
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db.init()

    await db.insert({ name: 'Alice', age: 30 })
    await db.close()

    // 2. Reopen without calling createIndex again
    db = DocumentDataply.Define<TestDoc>().Options({}).Open(DB_PATH)
    await db.init()

    // 3. Verify if index is preserved
    // This will throw if 'name' is not in indexedFields
    const results = await db.select({ name: 'Alice' }).drain()
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')

    await db.close()
  })
})
