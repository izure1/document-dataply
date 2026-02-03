
import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

type MassiveDoc = {
  id: number
  guid: string
  isActive: boolean
  balance: string
  age: number
  eyeColor: string
  name: string
  gender: string
  company: string
  email: string
  phone: string
  address: string
  registered: string
}

describe('Massive Data Insertion', () => {
  const dbPath = path.join(__dirname, 'test_massive.db')
  let db: DocumentDataply<MassiveDoc, {
    age: true
    gender: true
    company: true
  }>

  // Increase timeout for massive operation (5 minutes)
  jest.setTimeout(300000)

  beforeAll(async () => {
    // Cleanup before starting
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    // Cleanup index files if they exist (they usually have suffixes)
    const dir = path.dirname(dbPath)
    const files = fs.readdirSync(dir)
    for (const file of files) {
      if (file.startsWith('test_massive.db.')) {
        fs.unlinkSync(path.join(dir, file))
      }
    }
  })

  afterAll(async () => {
    if (db) await db.close()

    // Cleanup after finishing
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    const dir = path.dirname(dbPath)
    if (fs.existsSync(dir)) {
      const files = fs.readdirSync(dir)
      for (const file of files) {
        if (file.startsWith('test_massive.db.')) {
          fs.unlinkSync(path.join(dir, file))
        }
      }
    }
  })

  test('should insert 10,000 documents', async () => {
    db = DocumentDataply.Define<MassiveDoc>().Options({
      wal: dbPath + '.wal',
      indices: {
        age: true,
        gender: true,
        company: true
      }
    }).Open(dbPath)
    await db.init()

    const BATCH_SIZE = 1000
    const TOTAL_ITEMS = 10000

    console.log(`Starting massive insertion of ${TOTAL_ITEMS} items...`)
    const start = Date.now()

    for (let i = 0; i < TOTAL_ITEMS; i += BATCH_SIZE) {
      const tx = db.createTransaction()
      const documents: MassiveDoc[] = []
      for (let j = 0; j < BATCH_SIZE; j++) {
        const id = i + j
        const doc: MassiveDoc = {
          id: id,
          guid: `guid-${id}`,
          isActive: id % 2 === 0,
          balance: `$${id * 100}`,
          age: 20 + (id % 50),
          eyeColor: ['blue', 'brown', 'green'][id % 3],
          name: `User ${id}`,
          gender: id % 2 === 0 ? 'male' : 'female',
          company: `Company ${id % 100}`,
          email: `user${id}@example.com`,
          phone: `+1 ${id}`,
          address: `Address ${id}`,
          registered: new Date().toISOString()
        }
        documents.push(doc)
      }
      console.log(`Inserting batch ${i / BATCH_SIZE + 1}/${TOTAL_ITEMS / BATCH_SIZE}`)
      await db.insertBatch(documents, tx)
      console.log(`Inserted batch ${i / BATCH_SIZE + 1}/${TOTAL_ITEMS / BATCH_SIZE}`)
      await tx.commit()
      console.log(`Committed batch ${i / BATCH_SIZE + 1}/${TOTAL_ITEMS / BATCH_SIZE}`)
      documents.length = 0
    }

    const duration = Date.now() - start
    console.log(`Massive insertion took ${duration}ms`)

    const meta = await db.getMetadata()
    expect(meta.rowCount).toBe(TOTAL_ITEMS)
  })

  test('should retrieve data correctly after massive insertion', async () => {
    // Search by company (indexed)
    const results = await db.select({ company: 'Company 1' }).drain()
    // Logic: id % 100.
    // 0 to 999.
    // id 1 % 100 = 1.
    // id 101 % 100 = 1.
    // ...
    // id 9901 % 100 = 1.
    // Total count: 100 items. (0-99 -> 1 item, 100-199 -> 1 item... 100 groups of 100)

    expect(results.length).toBe(100)
    expect(results[0].company).toBe('Company 1')

    // Verify data integrity of a sample
    const sample = results[0]
    expect(sample).toBeDefined()
    expect(sample.company).toBe('Company 1')
  })

  test('should match equality query on indexed field (gender)', async () => {
    const males = await db.select({ gender: 'male' }).drain()
    expect(males.length).toBe(5000)
  })
})
