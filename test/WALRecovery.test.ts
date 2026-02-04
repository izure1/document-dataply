import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'
import { spawnSync } from 'node:child_process'

describe('DocumentDataply WAL Crash Recovery', () => {
  const testDir = path.join(__dirname, 'tmp_wal')
  const dbPath = path.join(testDir, 'crash.db')
  const walPath = path.join(testDir, 'crash.db.wal')
  const simScript = path.join(__dirname, 'wal_crash_sim_bundled.js')

  beforeEach(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true })
    }
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    // Clean up
  })

  function runSimulation(action: 'commit' | 'no-commit') {
    return spawnSync('node', [simScript, dbPath, walPath, action], {
      cwd: path.resolve(__dirname, '..'),
      shell: true,
      encoding: 'utf8'
    })
  }

  test('should recover committed data from WAL after crash', async () => {
    // 1. Run simulation that commits and crashes
    const result = runSimulation('commit')
    // We expect it to exit with 0 (simulated crash after commit)
    if (result.status !== 0) {
      console.error('Simulation Failed:', result.stderr)
    }
    expect(result.status).toBe(0)

    // 2. Open DB in this process
    // At this point, crash.db.wal should exist and contain the committed record
    const db = DocumentDataply.Define<{ name: string }>().Options({
      indices: { name: true },
      wal: walPath
    }).Open(dbPath)

    await db.init()

    // 3. Verify recovery
    const docs = await db.select({ name: 'CommittedData' }).drain()
    expect(docs.length).toBe(1)
    expect(docs[0].name).toBe('CommittedData')

    await db.close()
  })

  test('should NOT recover uncommitted data from WAL after crash', async () => {
    // 1. Run simulation that inserts but crashes WITHOUT commit
    const result = runSimulation('no-commit')
    expect(result.status).toBe(0)

    // 2. Open DB
    const db = DocumentDataply.Define<{ name: string }>().Options({
      indices: { name: true },
      wal: walPath
    }).Open(dbPath)

    await db.init()

    // 3. Verify it's empty
    const docs = await db.select({ name: 'UncommittedData' }).drain()
    expect(docs.length).toBe(0)

    await db.close()
  })
})
