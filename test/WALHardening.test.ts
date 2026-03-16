import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

describe('DocumentDataply WAL Hardening Test', () => {
  const testDir = path.join(__dirname, 'tmp_wal_hardening')
  const dbPath = path.join(testDir, 'hardened.db')
  const walPath = path.join(testDir, 'hardened.db.wal')

  beforeEach(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true })
    }
    fs.mkdirSync(testDir, { recursive: true })
  })

  test('should ignore partially written WAL entries (checksum failure)', async () => {
    const db = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      logLevel: 3,
    }).Open(dbPath)
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db.init();
    (db as any).api.analysisManager.close()
    await db.flushAnalysis()

    const sizeBefore = fs.statSync(walPath).size
    await db.withWriteTransaction(async (tx) => {
      await db.insert({ name: 'Data1' }, tx)
    })
    const sizeAfter = fs.statSync(walPath).size
    expect(sizeAfter).toBeGreaterThan(sizeBefore)

    const truncateSize = Math.floor(sizeBefore + (sizeAfter - sizeBefore) / 2)
    fs.truncateSync(walPath, truncateSize)
    console.log(`Original WAL Size: ${sizeAfter}, Truncated WAL Size: ${truncateSize}`)

    const db2 = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      logLevel: 3,
    }).Open(dbPath)
    await db2.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db2.init();
    (db2 as any).api.analysisManager.close()

    const docs = await db2.select({ name: 'Data1' }).drain()
    console.log('Recovered docs count (partial write):', docs.length)
    expect(docs.length).toBe(0)

    await db2.close()
  })

  test('should handle garbage data at the end of WAL', async () => {
    const db = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      logLevel: 3,
    }).Open(dbPath)
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db.init();
    (db as any).api.analysisManager.close()
    await db.flushAnalysis()

    await db.withWriteTransaction(async (tx) => {
      await db.insert({ name: 'GarbageTest' }, tx)
    })

    const originalSize = fs.statSync(walPath).size
    console.log('Original WAL Size (Garbage Test):', originalSize)

    fs.appendFileSync(walPath, Buffer.alloc(100, 0xff))
    console.log('Appended WAL Size:', fs.statSync(walPath).size)

    const db2 = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      logLevel: 3,
    }).Open(dbPath)
    await db2.createIndex('idx_name', { type: 'btree', fields: ['name'] })
    await db2.init();
    (db2 as any).api.analysisManager.close()

    const docs = await db2.select({ name: 'GarbageTest' }).drain()
    console.log('Recovered docs count (garbage appended):', docs.length)
    expect(docs.length).toBe(1)
    expect(docs[0].name).toBe('GarbageTest')

    await db2.close()
    await db.close()
  })
})
