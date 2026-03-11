import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

type CounterDoc = {
  name: string
  count: number
}

describe('DocumentDataply Concurrency Stress Test', () => {
  const dbPath = path.join(__dirname, 'test_concurrency.db')
  let db: DocumentDataply<CounterDoc>

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = DocumentDataply.Define<CounterDoc>().Options({ logLevel: 0 }).Open(dbPath)
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db.createIndex('idx_count', { type: 'btree', fields: ['count'] })
    await db.init()
  })

  afterEach(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
  })

  test('should handle multiple concurrent insertions', async () => {
    const numParallel = 10
    const opsPerParallel = 10
    const totalDocs = numParallel * opsPerParallel

    const promises = []
    for (let i = 0; i < numParallel; i++) {
      promises.push((async () => {
        for (let j = 0; j < opsPerParallel; j++) {
          await db.insert({
            name: `User_${i}_${j}`,
            count: i * opsPerParallel + j
          })
        }
      })())
    }

    await Promise.all(promises)

    const allDocs = await db.select({}).drain()
    expect(allDocs.length).toBe(totalDocs)

    // Verify randomness and order
    const sortedById = [...allDocs].sort((a, b) => a._id - b._id)
    for (let i = 0; i < totalDocs; i++) {
      expect(sortedById[i]._id).toBe(i + 1)
    }
  }, 120000)

  test('should handle concurrent updates to different documents', async () => {
    // Insert initial data
    const initialDocs = 100
    const ids = await db.insertBatch(
      Array.from({ length: initialDocs }, (_, i) => ({ name: `User_${i}`, count: 0 }))
    )

    const promises = ids.map(id => {
      // Each doc updated 10 times concurrently (simulated)
      return Promise.all(Array.from({ length: 10 }, async () => {
        await db.partialUpdate({ _id: id } as any, (doc) => ({
          count: doc.count + 1
        }))
      }))
    })

    await Promise.all(promises)

    const results = await db.select({}).drain()
    results.forEach(doc => {
      expect(doc.count).toBe(10)
    })
  }, 120000)

  test('should handle mixed insert/update/delete concurrently', async () => {
    // This is a heavy stress test
    const initialCount = 50
    await db.insertBatch(Array.from({ length: initialCount }, (_, i) => ({ name: `Initial_${i}`, count: i })))

    const tasks = []

    // Task 1: Continuous insertion
    tasks.push((async () => {
      for (let i = 0; i < 100; i++) {
        await db.insert({ name: `New_${i}`, count: 100 + i })
      }
    })())

    // Task 2: Continuous update
    tasks.push((async () => {
      for (let i = 0; i < 50; i++) {
        await db.partialUpdate({ name: { like: 'Initial_%' } } as any, (doc) => ({
          count: doc.count * 2
        }))
        // Small delay to allow interleaving
        await new Promise(r => setTimeout(r, 5))
      }
    })())

    // Task 3: Continuous deletion
    tasks.push((async () => {
      // Delete even numbered initial docs
      for (let i = 0; i < initialCount; i += 2) {
        await db.delete({ name: `Initial_${i}` } as any)
        await new Promise(r => setTimeout(r, 10))
      }
    })())

    await Promise.all(tasks)

    // Final sanity check
    const all = await db.select({}).drain()
    // Initial(50) - Deleted(25) + New(100) = 125
    expect(all.length).toBe(125)
  }, 120000)

  test('should block read operations during a write operation', async () => {
    // Insert initial data fast
    await db.insert({ name: 'Initial', count: 0 })

    const orderOfExecution: string[] = []

    // 1. Start a slow write operation (large batch insert)
    const largeBatch = Array.from({ length: 5000 }, (_, i) => ({ name: `Batch_${i}`, count: i }))

    // Promise for write
    const writePromise = (async () => {
      await db.insertBatch(largeBatch)
      orderOfExecution.push('write_completed')
    })()

    // 2. Start a read operation WITH macrotask yielding to test starvation
    await new Promise(r => setTimeout(r, 50))

    // 2. Start a read operation
    const readPromise = (async () => {
      const docs = await db.select({}).drain()
      orderOfExecution.push('read_completed')
      return docs
    })()

    await Promise.all([writePromise, readPromise])

    // 3. Verify that write completes before read if read is blocked/queued
    // If read is NOT blocked by write, 'read_completed' should be pushed before 'write_completed'
    expect(orderOfExecution).toEqual(['read_completed', 'write_completed'])
  }, 120000)

})
