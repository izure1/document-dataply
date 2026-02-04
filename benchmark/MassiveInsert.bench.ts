import { DocumentDataply } from '../src/core'
import * as path from 'node:path'
import { runBenchmark, printSummary, cleanupDb, BenchResult, saveResultsJson } from './bench_util'

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

const dbPath = path.join(__dirname, 'bench_massive.db')
const TOTAL_ITEMS = 10000
const BATCH_SIZE = 1000

async function main() {
  // Final summary will be calculated across 5 full cycles
  // Each cycle creates a new DB, inserts 10k items, queries, updates, and deletes.

  const cycleFn = async () => {
    cleanupDb(dbPath)

    const db = DocumentDataply.Define<MassiveDoc>().Options({
      wal: dbPath + '.wal',
      indices: {
        age: true,
        gender: true,
        company: true
      }
    }).Open(dbPath)
    await db.init()

    // 1. Insert Performance
    console.log(`  [1/5] Starting InsertBatch...`)
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
          phone: `+1 ${id}`, address: `Address ${id}`, registered: new Date().toISOString()
        })
      }
      await db.insertBatch(documents)
    }
    const endInsert = performance.now()
    resultsMap.insert.push(endInsert - startInsert)

    // 2. Select Performance (Indexed field)
    console.log(`  [2/5] Starting Indexed Select...`)
    const startSelect = performance.now()
    await db.select({ company: 'Company 1' }).drain()
    const endSelect = performance.now()
    resultsMap.select.push(endSelect - startSelect)

    // 3. Partial Update Performance
    console.log(`  [3/5] Starting Partial Update (Bulk)...`)
    const startPartial = performance.now()
    await db.partialUpdate({ company: 'Company 5' } as any, { isActive: false })
    const endPartial = performance.now()
    resultsMap.partialUpdate.push(endPartial - startPartial)

    // 4. Full Update Performance
    console.log(`  [4/5] Starting Full Update (Single)...`)
    const startFull = performance.now()
    await db.fullUpdate({ _id: 500 } as any, (doc) => ({ ...doc, balance: '$999,999' }))
    const endFull = performance.now()
    resultsMap.fullUpdate.push(endFull - startFull)

    // 5. Delete Performance
    console.log(`  [5/5] Starting Delete (Bulk)...`)
    const startDelete = performance.now()
    await db.delete({ company: 'Company 10' } as any)
    const endDelete = performance.now()
    resultsMap.delete.push(endDelete - startDelete)

    await db.close()
  }

  const resultsMap = {
    insert: [] as number[],
    select: [] as number[],
    partialUpdate: [] as number[],
    fullUpdate: [] as number[],
    delete: [] as number[]
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
    formatResult('InsertBatch (10k items)', resultsMap.insert),
    formatResult('Select (Indexed Equality)', resultsMap.select),
    formatResult('Partial Update (Bulk)', resultsMap.partialUpdate),
    formatResult('Full Update (Single)', resultsMap.fullUpdate),
    formatResult('Delete (Bulk)', resultsMap.delete)
  ]

  printSummary(finalResults)

  if (process.argv.includes('--json')) {
    saveResultsJson(finalResults, path.join(__dirname, 'benchmark-results.json'))
  }

  cleanupDb(dbPath)
}

main().catch(console.error)
