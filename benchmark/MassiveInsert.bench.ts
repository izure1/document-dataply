import { DocumentDataply } from '../src/core'
import type { DataplyDocument } from '../src/types'
import * as path from 'node:path'
import { printSummary, cleanupDb, BenchResult, saveResultsJson } from './bench_util'

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
  title: string
  content: string
  tags: string[]
}

const dbPath = path.join(__dirname, 'bench_massive.db')
const TOTAL_ITEMS = 5000
const BATCH_SIZE = 1000

async function main() {
  // Final summary will be calculated across 5 full cycles
  // Each cycle creates a new DB, inserts 5k items, queries, updates, and deletes.

  const cycleFn = async () => {
    cleanupDb(dbPath)

    const db = DocumentDataply.Define<MassiveDoc>().Options({
      wal: dbPath + '.wal',
      pageCacheCapacity: 100000
    }).Open(dbPath)
    await db.createIndex('age', { type: 'btree', fields: ['age'] })
    await db.createIndex('gender', { type: 'btree', fields: ['gender'] })
    await db.createIndex('company', { type: 'btree', fields: ['company'] })
    await db.createIndex('title', { type: 'fts', fields: 'title', tokenizer: 'whitespace' })
    await db.createIndex('content', { type: 'fts', fields: 'content', tokenizer: 'whitespace' })
    await db.init()

    // 1. Insert Performance
    console.log(`  [1/9] Starting InsertBatch...`)
    const startInsert = performance.now()
    for (let i = 0; i < TOTAL_ITEMS; i += BATCH_SIZE) {
      const documents: MassiveDoc[] = []
      for (let j = 0; j < BATCH_SIZE; j++) {
        const id = i + j
        documents.push({
          id, guid: `guid-${id}`, isActive: id % 2 === 0, balance: `$${id * 100}`,
          age: 20 + (id % 50), eyeColor: ['blue', 'brown', 'green'][id % 3],
          name: `User ${id}`, gender: id % 2 === 0 ? 'male' : 'female',
          company: `Company ${id % 100}`, email: `user${id}@example.com`,
          phone: `+1 ${id}`, address: `Address ${id}`, registered: new Date().toISOString(),
          title: `Document Title ${id}`,
          content: `This is the content of document number ${id}. It contains some keywords for full text search testing.`,
          tags: [`tag${id % 10}`, `category${id % 5}`]
        })
      }
      await db.insertBatch(documents)
    }
    const endInsert = performance.now()
    resultsMap.insert.push(endInsert - startInsert)

    // 2. Select Performance (Indexed field)
    console.log(`  [2/9] Starting Indexed Select...`)
    const startSelect = performance.now()
    await db.select({ company: 'Company 1' }).drain()
    const endSelect = performance.now()
    resultsMap.select.push(endSelect - startSelect)

    // 3. Partial Update Performance
    console.log(`  [3/9] Starting Partial Update (Bulk)...`)
    const startPartial = performance.now()
    await db.partialUpdate({ company: 'Company 5' } as any, { isActive: false })
    const endPartial = performance.now()
    resultsMap.partialUpdate.push(endPartial - startPartial)

    // 4. Full Update Performance
    console.log(`  [4/9] Starting Full Update (Single)...`)
    const startFull = performance.now()
    await db.fullUpdate({ _id: 500 } as any, (doc) => ({ ...doc, balance: '$999,999' }))
    const endFull = performance.now()
    resultsMap.fullUpdate.push(endFull - startFull)

    // 5. Upsert (Insert Path - Single)
    console.log(`  [5/9] Starting Upsert (Insert Path - Single)...`)
    const startUpsertInsert = performance.now()
    await db.upsert({
      id: 999999, guid: 'guid-999999', isActive: true, balance: '$500',
      age: 30, eyeColor: 'blue', name: 'Upsert New User', gender: 'male',
      company: 'Company 1', email: 'upsert@example.com', phone: '+1 9999',
      address: 'Address 9999', registered: new Date().toISOString(),
      title: 'Upsert Title', content: 'Upsert content', tags: ['upsert']
    } as DataplyDocument<MassiveDoc>)
    const endUpsertInsert = performance.now()
    resultsMap.upsertInsert.push(endUpsertInsert - startUpsertInsert)

    // 6. Upsert (Update Path - Single)
    console.log(`  [6/10] Starting Upsert (Update Path - Single)...`)
    const startUpsertUpdate = performance.now()
    await db.upsert({
      _id: 100,
      id: 100, guid: 'guid-100-updated', isActive: false, balance: '$1,000,000',
      age: 35, eyeColor: 'green', name: 'User 100 Upserted', gender: 'female',
      company: 'Company 1', email: 'user100_upserted@example.com', phone: '+1 100',
      address: 'Address 100 Updated', registered: new Date().toISOString(),
      title: 'Document Title 100 Updated', content: 'Updated content via upsert',
      tags: ['tag100', 'upserted']
    } as DataplyDocument<MassiveDoc>)
    const endUpsertUpdate = performance.now()
    resultsMap.upsertUpdate.push(endUpsertUpdate - startUpsertUpdate)

    // 7. UpsertBatch (Mixed Insert & Update Batch)
    console.log(`  [7/10] Starting UpsertBatch (Mixed 100 items)...`)
    const startUpsertBatch = performance.now()
    const mixedBatch: DataplyDocument<MassiveDoc>[] = []
    for (let k = 1; k <= 50; k++) {
      mixedBatch.push({
        _id: k,
        id: k, guid: `guid-${k}-upserted`, isActive: true, balance: '$888',
        age: 30, eyeColor: 'blue', name: `User ${k} Upserted`, gender: 'male',
        company: 'Company 2', email: `user${k}_upserted@example.com`, phone: `+1 ${k}`,
        address: `Address ${k}`, registered: new Date().toISOString(),
        title: `Title ${k}`, content: `Content ${k}`, tags: ['batch']
      } as DataplyDocument<MassiveDoc>)
    }
    for (let k = 10001; k <= 10050; k++) {
      mixedBatch.push({
        id: k, guid: `guid-${k}`, isActive: false, balance: '$555',
        age: 25, eyeColor: 'green', name: `New User ${k}`, gender: 'female',
        company: 'Company 3', email: `newuser${k}@example.com`, phone: `+1 ${k}`,
        address: `Address ${k}`, registered: new Date().toISOString(),
        title: `Title ${k}`, content: `Content ${k}`, tags: ['new']
      } as DataplyDocument<MassiveDoc>)
    }
    await db.upsertBatch(mixedBatch)
    const endUpsertBatch = performance.now()
    resultsMap.upsertBatch.push(endUpsertBatch - startUpsertBatch)

    // 8. Delete Performance
    console.log(`  [8/10] Starting Delete (Bulk)...`)
    const startDelete = performance.now()
    await db.delete({ company: 'Company 10' } as any)
    const endDelete = performance.now()
    resultsMap.delete.push(endDelete - startDelete)

    // 9. FTS Single Keyword Search
    console.log(`  [9/10] Starting FTS Single Keyword Search...`)
    const startSearchSingle = performance.now()
    await db.select({ content: { match: 'content' } } as any).drain()
    const endSearchSingle = performance.now()
    resultsMap.ftsSearchSingle.push(endSearchSingle - startSearchSingle)

    // 10. FTS Multi Keyword Search
    console.log(`  [10/10] Starting FTS Multi Keyword Search...`)
    const startSearchMulti = performance.now()
    await db.select({ content: { match: 'document number' } } as any).drain()
    const endSearchMulti = performance.now()
    resultsMap.ftsSearchMulti.push(endSearchMulti - startSearchMulti)

    await db.close()
  }

  const resultsMap = {
    insert: [] as number[],
    select: [] as number[],
    partialUpdate: [] as number[],
    fullUpdate: [] as number[],
    upsertInsert: [] as number[],
    upsertUpdate: [] as number[],
    upsertBatch: [] as number[],
    delete: [] as number[],
    ftsSearchSingle: [] as number[],
    ftsSearchMulti: [] as number[]
  }

  console.log(`Starting 5 iterations of full lifecycle benchmarks (${TOTAL_ITEMS} docs)...`)

  for (let i = 0; i < 5; i++) {
    console.log(`\nIteration ${i + 1}/5`)
    await cycleFn()
  }

  const formatResult = (name: string, times: number[]): BenchResult => {
    const avg = times.reduce((a, b) => a + b, 0) / times.length
    return { name, times, avg, min: Math.min(...times), max: Math.max(...times) }
  }

  const finalResults = [
    formatResult('InsertBatch (5k items)', resultsMap.insert),
    formatResult('Select (Indexed Equality)', resultsMap.select),
    formatResult('Partial Update (Bulk)', resultsMap.partialUpdate),
    formatResult('Full Update (Single)', resultsMap.fullUpdate),
    formatResult('Upsert (Insert Single)', resultsMap.upsertInsert),
    formatResult('Upsert (Update Single)', resultsMap.upsertUpdate),
    formatResult('UpsertBatch (Mixed 100 items)', resultsMap.upsertBatch),
    formatResult('Delete (Bulk)', resultsMap.delete),
    formatResult('FtsSearch (Single Keyword)', resultsMap.ftsSearchSingle),
    formatResult('FtsSearch (Multi Keyword)', resultsMap.ftsSearchMulti)
  ]


  printSummary(finalResults)

  if (process.argv.includes('--json')) {
    saveResultsJson(finalResults, path.join(__dirname, 'benchmark-results.json'))
  }

  cleanupDb(dbPath)
}

main().catch(console.error)
