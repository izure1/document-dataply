import { DocumentDataply } from '../src/core/document'
import * as fs from 'fs'
import * as path from 'path'

type UserDoc = {
  name?: string
  age?: number
  city?: string
  user?: {
    profile: {
      nickname: string
      level: number
    }
  }
  tags?: string[]
}

describe('DocumentDataply Basic CRUD', () => {
  const dbPath = path.join(__dirname, 'test_basic.db')
  let db: DocumentDataply<UserDoc>

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = new DocumentDataply(dbPath)
    await db.init()
  })

  afterEach(async () => {
    await db.close()
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
  })

  test('should insert and select a document', async () => {
    const doc = { name: 'John Doe', age: 30, city: 'Seoul' }
    const success = await db.insert(doc)
    expect(success).toBe(1)

    const results = await db.select({ name: 'John Doe' })
    expect(results.length).toBe(1)
    expect(results[0]).toMatchObject(doc)
    expect(results[0]._id).toBeDefined()
  })

  test('should insert multiple documents and select them', async () => {
    await db.insert({ name: 'Jane Doe', age: 25, city: 'Busan' })
    await db.insert({ name: 'Jim Beam', age: 40, city: 'Seoul' })

    const seoulites = await db.select({ city: 'Seoul' })
    expect(seoulites.length).toBe(1)

    const busanite = await db.select({ city: 'Busan' })
    expect(busanite.length).toBe(1)
    expect(busanite[0].name).toBe('Jane Doe')
  })

  test('should handle nested objects', async () => {
    const nestedDoc = {
      user: {
        profile: {
          nickname: 'iz-ure',
          level: 99
        }
      },
      tags: ['cool', 'test']
    }
    await db.insert(nestedDoc)

    const result = await db.select({ 'user.profile.nickname': 'iz-ure' })
    expect(result.length).toBe(1)
    expect(result[0].user?.profile.level).toBe(99)
  })

  test('should support array index access in query', async () => {
    const docWithArray = {
      name: 'Array Test',
      tags: ['a', 'b', 'c']
    }
    await db.insert(docWithArray)

    // 'tags.0' is 'a'
    const result0 = await db.select({ 'tags.0': 'a' })
    expect(result0.length).toBe(1)
    expect(result0[0].name).toBe('Array Test')

    // 'tags.1' is 'b'
    const result1 = await db.select({ 'tags.1': 'b' })
    expect(result1.length).toBe(1)

    // 'tags.5' should return 0 results
    const result5 = await db.select({ 'tags.5': 'x' })
    expect(result5.length).toBe(0)
  })

  test('should support like operator', async () => {
    await db.insert({ name: 'John Doe', age: 30, city: 'Seoul' })
    await db.insert({ name: 'Jane Doe', age: 25, city: 'Busan' })

    const results = await db.select({ name: { like: '% Doe' } })
    expect(results.length).toBe(2)
  })

  test('should get metadata', async () => {
    const metadata = await db.getMetadata()
    expect(metadata.rowCount).toBeGreaterThanOrEqual(0)
    expect(metadata.pageSize).toBeDefined()
  })
})
