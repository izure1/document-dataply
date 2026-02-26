import { DocumentDataply } from '../src/core'
import * as path from 'node:path'
import { runBenchmark, printSummary, cleanupDb, BenchResult, saveResultsJson } from './bench_util'

type FtsDoc = {
  id: number
  title: string
  content: string
  tags: string[]
}

const dbPath = path.join(__dirname, 'bench_fts.db')
const TOTAL_ITEMS = 10000
const BATCH_SIZE = 1000

async function main() {
  const resultsMap = {
    insert: [] as number[],
    searchSingle: [] as number[],
    searchMulti: [] as number[]
  }

  const cycleFn = async () => {
    cleanupDb(dbPath)

    const db = DocumentDataply.Define<FtsDoc>().Options({
      wal: dbPath + '.wal',
      pageCacheCapacity: 100000,
      indices: {
        title: { type: 'fts', tokenizer: 'whitespace' },
        content: { type: 'fts', tokenizer: 'whitespace' }
      }
    }).Open(dbPath)
    await db.init()

    // 1. FTS Indexing Performance (Bulk Insert)
    console.log(`  [1/3] Starting FTS InsertBatch (${TOTAL_ITEMS} items)...`)
    const startInsert = performance.now()
    for (let i = 0; i < TOTAL_ITEMS; i += BATCH_SIZE) {
      const documents: FtsDoc[] = []
      for (let j = 0; j < BATCH_SIZE; j++) {
        const id = i + j
        documents.push({
          id,
          title: `Document Title ${id}`,
          content: `This is the content of document number ${id}. It contains some keywords for full text search testing.`,
          tags: [`tag${id % 10}`, `category${id % 5}`]
        })
      }
      await db.insertBatch(documents)
    }
    const endInsert = performance.now()
    resultsMap.insert.push(endInsert - startInsert)

    // 2. FTS Single Keyword Search
    console.log(`  [2/3] Starting FTS Single Keyword Search...`)
    const startSearchSingle = performance.now()
    // "content" keyword appears in every document content
    await db.select({ content: { match: 'content' } } as any).drain()
    const endSearchSingle = performance.now()
    resultsMap.searchSingle.push(endSearchSingle - startSearchSingle)

    // 3. FTS Multi Keyword Search
    console.log(`  [3/3] Starting FTS Multi Keyword Search...`)
    const startSearchMulti = performance.now()
    // Searching for "document" and "number" which appear in every document content
    await db.select({ content: { match: 'document number' } } as any).drain()
    const endSearchMulti = performance.now()
    resultsMap.searchMulti.push(endSearchMulti - startSearchMulti)

    await db.close()
  }

  console.log(`Starting 5 iterations of FTS benchmarks (${TOTAL_ITEMS} docs)...`)

  for (let i = 0; i < 5; i++) {
    console.log(`\nIteration ${i + 1}/5`)
    await cycleFn()
  }

  const formatResult = (name: string, times: number[]): BenchResult => {
    const avg = times.reduce((a, b) => a + b, 0) / times.length
    return { name, times, avg, min: Math.min(...times), max: Math.max(...times) }
  }

  const finalResults = [
    formatResult('FtsInsertBatch (10k items)', resultsMap.insert),
    formatResult('FtsSearch (Single Keyword)', resultsMap.searchSingle),
    formatResult('FtsSearch (Multi Keyword)', resultsMap.searchMulti)
  ]

  printSummary(finalResults)

  if (process.argv.includes('--json')) {
    saveResultsJson(finalResults, path.join(__dirname, 'benchmark-results.json'))
  }

  cleanupDb(dbPath)
}

main().catch(console.error)
