import { DocumentDataply } from '../src/core'
import * as path from 'node:path'

async function run() {
  const dbPath = process.argv[2]
  const walPath = process.argv[3]
  const action = process.argv[4] // 'commit' or 'no-commit'

  const db = DocumentDataply.Define<{ name: string }>().Options({
    indices: { name: true },
    wal: walPath
  }).Open(dbPath)

  await db.init()

  if (action === 'commit') {
    await db.insert({ name: 'CommittedData' })
    // No close(), just exit to simulate crash after commit
    process.exit(0)
  } else {
    const tx = db.createTransaction()
    await db.insert({ name: 'UncommittedData' }, tx)
    // Exit before commit
    process.exit(0)
  }
}

run().catch(err => {
  console.error(err)
  process.exit(1)
})
