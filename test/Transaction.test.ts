import { DocumentDataply } from '../src/core/document'
import * as fs from 'fs'
import * as path from 'path'

type TxDoc = {
  name: string
}

describe('DocumentDataply Transaction', () => {
  const dbPath = path.join(__dirname, 'test_tx.db')
  let db: DocumentDataply<TxDoc>

  beforeAll(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = new DocumentDataply(dbPath, {
      indecies: {
        name: true,
      }
    })
    await db.init()
  })

  afterAll(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
  })

  test('should commit transaction', async () => {
    const tx = db.createTransaction()
    await db.insert({ name: 'Tx User 1' }, tx)
    await tx.commit()

    const results = await db.select({ name: 'Tx User 1' })
    expect(results.length).toBe(1)
  })

  test('should rollback transaction', async () => {
    const tx = db.createTransaction()
    await db.insert({ name: 'Tx User 2' }, tx)
    await tx.rollback()

    const results = await db.select({ name: 'Tx User 2' })
    expect(results.length).toBe(0)
  })

  test('should not see uncommitted data from other transactions (basic isolation check)', async () => {
    // Note: Depends on dataply internal isolation implementation
    const tx = db.createTransaction()
    await db.insert({ name: 'Isolated User' }, tx)

    const results = await db.select({ name: 'Isolated User' }) // This runs without 'tx', so it shouldn't see it (if isolation is set)
    expect(results.length).toBe(0)

    await tx.commit()
    const finalResults = await db.select({ name: 'Isolated User' })
    expect(finalResults.length).toBe(1)
  })
})
