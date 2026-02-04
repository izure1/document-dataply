import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'tmp_full_update_debug')
const DB_PATH = path.join(TEST_DIR, 'test.ply')

type MassiveDoc = {
  id: number
  balance: string
  age: number
  company: string
  isActive: boolean
}

const TOTAL_ITEMS = 10000  // Same as benchmark
const BATCH_SIZE = 1000

describe('Debug fullUpdate sequence', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  afterEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
  })

  test('debug: check exactly where it hangs', async () => {
    const db = DocumentDataply.Define<MassiveDoc>().Options({
      indices: {
        age: true,
        company: true
      }
    }).Open(DB_PATH)

    await db.init()
    console.log('1. Database initialized.')

    // Insert documents
    for (let i = 0; i < TOTAL_ITEMS; i += BATCH_SIZE) {
      const documents: MassiveDoc[] = []
      for (let j = 0; j < BATCH_SIZE; j++) {
        const id = i + j
        documents.push({
          id, balance: `$${id * 100}`,
          age: 20 + (id % 50),
          company: `Company ${id % 100}`,
          isActive: id % 2 === 0
        })
      }
      await db.insertBatch(documents)
    }
    console.log('2. Insert complete.')

    // Test 1: select with _id (should use getKeys with _id)
    console.log('3. Testing select with _id...')
    const selectResult = await db.select({ _id: 50 } as any).drain()
    console.log(`4. Select result: ${selectResult.length} docs`)

    // Test 2: partialUpdate
    console.log('5. Testing partialUpdate...')
    const partialResult = await db.partialUpdate({ company: 'Company 5' } as any, { isActive: false })
    console.log(`6. PartialUpdate result: ${partialResult} docs`)

    // Test 3: select with _id after partialUpdate
    console.log('7. Testing select with _id after partialUpdate...')
    const selectResult2 = await db.select({ _id: 50 } as any).drain()
    console.log(`8. Select result: ${selectResult2.length} docs`)

    // Test 4: fullUpdate with _id
    console.log('9. Testing fullUpdate with _id...')
    const fullResult = await db.fullUpdate({ _id: 50 } as any, (doc) => ({ ...doc, balance: '$999' }))
    console.log(`10. FullUpdate result: ${fullResult} docs`)

    await db.close()
    console.log('11. Test complete.')
  }, 30000)
})
