import { DocumentDataply } from '../src/core'
import { DataplyDocument } from '../src/types'
import * as path from 'node:path'
import { cleanupDb, BenchResult, printSummary } from './bench_util'

type BenchDoc = {
  name: string
  score: number
  category: string
}

const dbPath = path.join(__dirname, 'bench_upsert.db')
const ITEM_COUNT = 1000

async function runUpsertBenchmark() {
  const results: BenchResult[] = []

  // 1. Upsert Insert Path Benchmark (1,000 items)
  const insertTimes: number[] = []
  for (let cycle = 0; cycle < 3; cycle++) {
    cleanupDb(dbPath)
    const db = DocumentDataply.Define<BenchDoc>().Options({ logLevel: 3 }).Open(dbPath)
    await db.createIndex('idx_score', { type: 'btree', fields: ['score'] })
    await db.init()

    const start = performance.now()
    for (let i = 0; i < ITEM_COUNT; i++) {
      await db.upsert({ name: `User_${i}`, score: i * 10, category: 'A' } as DataplyDocument<BenchDoc>)
    }
    const end = performance.now()
    insertTimes.push(end - start)

    await db.close()
  }

  results.push({
    name: 'Upsert (Insert Path 1k ops)',
    times: insertTimes,
    avg: insertTimes.reduce((a, b) => a + b, 0) / insertTimes.length,
    min: Math.min(...insertTimes),
    max: Math.max(...insertTimes)
  })

  // 2. Upsert Update Path Benchmark (1,000 items update)
  const updateTimes: number[] = []
  for (let cycle = 0; cycle < 3; cycle++) {
    cleanupDb(dbPath)
    const db = DocumentDataply.Define<BenchDoc>().Options({ logLevel: 3 }).Open(dbPath)
    await db.createIndex('idx_score', { type: 'btree', fields: ['score'] })
    await db.init()

    // 1,000개 사전 생성
    const ids: number[] = []
    for (let i = 0; i < ITEM_COUNT; i++) {
      const id = await db.insert({ name: `User_${i}`, score: i * 10, category: 'A' })
      ids.push(id)
    }

    const start = performance.now()
    for (let i = 0; i < ITEM_COUNT; i++) {
      await db.upsert({ _id: ids[i], name: `User_${i}_Updated`, score: i * 10 + 1, category: 'B' })
    }
    const end = performance.now()
    updateTimes.push(end - start)

    await db.close()
  }

  results.push({
    name: 'Upsert (Update Path 1k ops)',
    times: updateTimes,
    avg: updateTimes.reduce((a, b) => a + b, 0) / updateTimes.length,
    min: Math.min(...updateTimes),
    max: Math.max(...updateTimes)
  })

  printSummary(results)
  cleanupDb(dbPath)
}

runUpsertBenchmark().catch(console.error)
