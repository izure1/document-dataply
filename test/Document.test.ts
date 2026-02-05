import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

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
  let db: DocumentDataply<UserDoc, {
    name: true
    age: true
    city: true
    'user.profile.nickname': true
    'user.profile.level': true
    'tags.0': true
    'tags.1': true
    'tags.5': true
  }>

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = DocumentDataply.Define<UserDoc>().Options({
      indices: {
        name: true,
        age: true,
        city: true,
        'user.profile.nickname': true,
        'user.profile.level': true,
        'tags.0': true,
        'tags.1': true,
        'tags.5': true,
      }
    }).Open(dbPath)
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

    const results = await db.select({ name: 'John Doe' }).drain()
    expect(results.length).toBe(1)
    expect(results[0]).toMatchObject(doc)
    expect(results[0]._id).toBeDefined()
  })

  test('should insert multiple documents and select them', async () => {
    await db.insert({ name: 'Jane Doe', age: 25, city: 'Busan' })
    await db.insert({ name: 'Jim Beam', age: 40, city: 'Seoul' })

    const seoulites = await db.select({ city: 'Seoul' }).drain()
    expect(seoulites.length).toBe(1)

    const busanite = await db.select({ city: 'Busan' }).drain()
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

    const result = await db.select({ 'user.profile.nickname': 'iz-ure' }).drain()
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
    const result0 = await db.select({ 'tags.0': 'a' }).drain()
    expect(result0.length).toBe(1)
    expect(result0[0].name).toBe('Array Test')

    // 'tags.1' is 'b'
    const result1 = await db.select({ 'tags.1': 'b' }).drain()
    expect(result1.length).toBe(1)

    // 'tags.5' should return 0 results
    const result5 = await db.select({ 'tags.5': 'x' }).drain()
    expect(result5.length).toBe(0)
  })

  test('should support like operator', async () => {
    await db.insert({ name: 'John Doe', age: 30, city: 'Seoul' })
    await db.insert({ name: 'Jane Doe', age: 25, city: 'Busan' })

    const results = await db.select({ name: { like: '% Doe' } }).drain()
    expect(results.length).toBe(2)
  })

  test('should get metadata', async () => {
    const metadata = await db.getMetadata()
    expect(metadata.rowCount).toBeGreaterThanOrEqual(0)
    expect(metadata.pageSize).toBeDefined()
  })

  describe('count() method', () => {
    test('should return 0 for empty collection', async () => {
      const count = await db.count({})
      expect(count).toBe(0)
    })

    test('should return total count with empty query', async () => {
      await db.insert({ name: 'User 1', age: 20 })
      await db.insert({ name: 'User 2', age: 30 })
      await db.insert({ name: 'User 3', age: 40 })

      const count = await db.count({})
      expect(count).toBe(3)
    })

    test('should count with single index filter', async () => {
      await db.insert({ name: 'John', city: 'Seoul' })
      await db.insert({ name: 'Jane', city: 'Busan' })
      await db.insert({ name: 'Jim', city: 'Seoul' })

      const seoulCount = await db.count({ city: 'Seoul' })
      expect(seoulCount).toBe(2)

      const busanCount = await db.count({ city: 'Busan' })
      expect(busanCount).toBe(1)
    })

    test('should count with multiple index filters', async () => {
      await db.insert({ name: 'User A', age: 25, city: 'Seoul' })
      await db.insert({ name: 'User B', age: 25, city: 'Busan' })
      await db.insert({ name: 'User C', age: 30, city: 'Seoul' })

      const count = await db.count({ age: 25, city: 'Seoul' })
      expect(count).toBe(1)
    })

    test('should return 0 when no documents match', async () => {
      await db.insert({ name: 'User A', age: 25 })
      const count = await db.count({ age: 100 })
      expect(count).toBe(0)
    })

    test('should reflect changes after delete and update', async () => {
      await db.insert({ name: 'User X', age: 10, city: 'Seoul' })
      await db.insert({ name: 'User Y', age: 10, city: 'Seoul' })

      expect(await db.count({ age: 10 })).toBe(2)

      // Delete one
      await db.delete({ name: 'User X' })
      expect(await db.count({ age: 10 })).toBe(1)

      // Update one
      await db.partialUpdate({ name: 'User Y' }, { age: 20 })
      expect(await db.count({ age: 10 })).toBe(0)
      expect(await db.count({ age: 20 })).toBe(1)
    })
  })

  describe('Pagination (offset)', () => {
    beforeEach(async () => {
      // 5 Documents for pagination testing
      await db.insert({ name: 'User 1', age: 10 })
      await db.insert({ name: 'User 2', age: 20 })
      await db.insert({ name: 'User 3', age: 30 })
      await db.insert({ name: 'User 4', age: 40 })
      await db.insert({ name: 'User 5', age: 50 })
    })

    test('should skip documents using offset', async () => {
      const results = await db.select({}, { offset: 2 }).drain()
      expect(results.length).toBe(3)
      expect(results[0].name).toBe('User 3')
      expect(results[2].name).toBe('User 5')
    })

    test('should combine offset and limit', async () => {
      const results = await db.select({}, { offset: 1, limit: 2 }).drain()
      expect(results.length).toBe(2)
      expect(results[0].name).toBe('User 2')
      expect(results[1].name).toBe('User 3')
    })

    test('should support offset with in-memory sorting (orderBy)', async () => {
      // city is indexed, but using it with age (also indexed) but sorting by name (indexed)
      // to trigger the in-memory sorting logic path in select()
      const results = await db.select({}, { orderBy: 'name', sortOrder: 'desc', offset: 1, limit: 2 }).drain()
      expect(results.length).toBe(2)
      expect(results[0].name).toBe('User 4')
      expect(results[1].name).toBe('User 3')
    })

    test('should return empty array if offset exceeds count', async () => {
      const results = await db.select({}, { offset: 10 }).drain()
      expect(results.length).toBe(0)
    })
  })
})
