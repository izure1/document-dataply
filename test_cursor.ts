import { DocumentDataply } from './src/core'
import * as fs from 'node:fs'

async function test() {
  const db = DocumentDataply.Define<any>().Options({}).Open('test_cursor')
  await db.init()

  if (!fs.existsSync('test_cursor')) {
    fs.unlinkSync('test_cursor')
    await db.createIndex('idx_nickname_createdat', { type: 'btree', fields: ['nickname', 'createdat'] })
    console.log("Inserting 20,000 records...")
    const docs = []
    for (let i = 0; i < 10000; i++) {
      docs.push({ nickname: 'Winv2', createdat: 1000000 + i })
    }
    for (let i = 0; i < 10000; i++) {
      docs.push({ nickname: 'Other', createdat: 1000000 + i })
    }
    await db.insertBatch(docs)
  }

  // Warmup
  await db.select(
    {
      nickname: 'Winv2',
      createdat: { lte: 1010000 }
    },
    {
      limit: 100,
      sortOrder: 'desc',
      orderBy: 'createdat'
    }
  ).drain()

  console.log("\n=== Test: 일치 10000행 ===")
  let s = performance.now()
  const r1 = await db.select(
    {
      nickname: 'Winv2',
      createdat: {
        lte: 1010000
      }
    },
    {
      limit: 100,
      sortOrder: 'desc',
      orderBy: 'createdat'
    }
  ).drain()
  let e = performance.now()
  console.log(`Total: ${(e - s).toFixed(2)}ms (${r1.length}개)`)

  console.log("\n=== Test: 일치 101행 ===")
  s = performance.now()
  const r2 = await db.select(
    {
      nickname: 'Winv2',
      createdat: {
        lte: 1000100
      }
    },
    {
      limit: 100,
      sortOrder: 'desc',
      orderBy: 'createdat'
    }
  ).drain()
  e = performance.now()
  console.log(`Total: ${(e - s).toFixed(2)}ms (${r2.length}개)`)

  await db.close()
}

test().catch(console.error)
