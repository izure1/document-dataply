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
      indices: { name: true }
    }).Open(dbPath)
    await db.init()

    // Use a second transaction to prevent WAL from being cleared after Tx1 commits
    const txKeepWal = db.createTransaction()

    const tx = db.createTransaction()
    await db.insert({ name: 'Data1' }, tx)
    await tx.commit()

    expect(fs.existsSync(walPath)).toBe(true)
    const originalSize = fs.statSync(walPath).size
    console.log('Original WAL Size:', originalSize)
    expect(originalSize).toBeGreaterThan(0)

    fs.truncateSync(walPath, originalSize - 5)
    console.log('Truncated WAL Size:', fs.statSync(walPath).size)

    const db2 = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      indices: { name: true }
    }).Open(dbPath)
    await db2.init()

    const docs = await db2.select({ name: 'Data1' }).drain()
    console.log('Recovered docs count (partial write):', docs.length)
    expect(docs.length).toBe(0)

    await db2.close()
    await db.close()
  })

  test('should handle garbage data at the end of WAL', async () => {
    const db = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      indices: { name: true }
    }).Open(dbPath)
    await db.init()

    // Use a second transaction to prevent WAL from being cleared after Tx1 commits
    const txKeepWal = db.createTransaction()

    const tx = db.createTransaction()
    await db.insert({ name: 'GarbageTest' }, tx)
    await tx.commit()

    const originalSize = fs.statSync(walPath).size
    console.log('Original WAL Size (Garbage Test):', originalSize)

    fs.appendFileSync(walPath, Buffer.alloc(100, 0xff))
    console.log('Appended WAL Size:', fs.statSync(walPath).size)

    const db2 = DocumentDataply.Define<{ name: string }>().Options({
      wal: walPath,
      indices: { name: true }
    }).Open(dbPath)
    await db2.init()

    const docs = await db2.select({ name: 'GarbageTest' }).drain()
    console.log('Recovered docs count (garbage appended):', docs.length)
    expect(docs.length).toBe(1)
    expect(docs[0].name).toBe('GarbageTest')

    await db2.close()
    await db.close()
  })
})
