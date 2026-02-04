import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

type TagDoc = {
  name: string
  tags: string[]
}

describe('DocumentDataply Array Index Shift Test', () => {
  const dbPath = path.join(__dirname, 'test_arrayshift.db')
  let db: DocumentDataply<TagDoc, {
    name: true
    'tags.0': true
    'tags.1': true
  }>

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    db = DocumentDataply.Define<TagDoc>().Options({
      indices: {
        name: true,
        'tags.0': true,
        'tags.1': true
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

  test('should update index when array elements shift due to deletion', async () => {
    // 1. Initial insert
    await db.insert({ name: 'ShiftTest', tags: ['a', 'b', 'c'] })

    // Verify initial state
    let res0 = await db.select({ 'tags.0': 'a' }).drain()
    expect(res0.length).toBe(1)
    let res1 = await db.select({ 'tags.1': 'b' }).drain()
    expect(res1.length).toBe(1)

    // 2. Remove first element -> ['b', 'c']
    // 'b' moves from tags.1 to tags.0
    await db.partialUpdate({ name: 'ShiftTest' } as any, (doc) => ({
      tags: doc.tags.slice(1)
    }))

    // Verify shift
    // tags.0 should now be 'b'
    res0 = await db.select({ 'tags.0': 'b' }).drain()
    expect(res0.length).toBe(1)
    expect(res0[0].tags).toEqual(['b', 'c'])

    // tags.0 should no longer be 'a'
    res0 = await db.select({ 'tags.0': 'a' }).drain()
    expect(res0.length).toBe(0)

    // tags.1 should now be 'c'
    res1 = await db.select({ 'tags.1': 'c' }).drain()
    expect(res1.length).toBe(1)

    // tags.1 should no longer be 'b'
    res1 = await db.select({ 'tags.1': 'b' }).drain()
    expect(res1.length).toBe(0)
  })

  test('should update index when array elements shift due to insertion at start', async () => {
    await db.insert({ name: 'InsertTest', tags: ['x', 'y'] })

    // Insert 'w' at start -> ['w', 'x', 'y']
    await db.partialUpdate({ name: 'InsertTest' } as any, (doc) => ({
      tags: ['w', ...doc.tags]
    }))

    // tags.0: 'w'
    const res0 = await db.select({ 'tags.0': 'w' }).drain()
    expect(res0.length).toBe(1)

    // tags.1: 'x' (moved from tags.0)
    const res1 = await db.select({ 'tags.1': 'x' }).drain()
    expect(res1.length).toBe(1)
  })

  test('should handle completely replacing array with shorter one', async () => {
    await db.insert({ name: 'ReplaceTest', tags: ['1', '2'] })

    // Replace with ['3']
    await db.partialUpdate({ name: 'ReplaceTest' } as any, { tags: ['3'] })

    // tags.0: '3'
    expect((await db.select({ 'tags.0': '3' }).drain()).length).toBe(1)
    // tags.0: '1' (gone)
    expect((await db.select({ 'tags.0': '1' }).drain()).length).toBe(0)
    // tags.1: '2' (gone)
    expect((await db.select({ 'tags.1': '2' }).drain()).length).toBe(0)
  })
})
