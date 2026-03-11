import { DocumentDataply } from '../src/core'
import * as fs from 'node:fs'
import * as path from 'node:path'

const TEST_DIR = path.join(__dirname, 'tmp_restart_check')
const DB_PATH = path.join(TEST_DIR, 'restart_test.ply')

type ChatDoc = {
  streamer: string
  nickname: string
  msg: string
  created_at: number
}

describe('Composite Index Restart Verification', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR, { recursive: true })
  })

  test('Should recover composite index correctly after restart', async () => {
    // 1. 초기화 및 복합 인덱스 설정
    let db = DocumentDataply.Define<ChatDoc>().Options({ logLevel: 3 }).Open(DB_PATH)
    await db.init()

    // 복합 인덱스 생성 (4개 필드 조합)
    await db.createIndex('idx_chat_composite', {
      type: 'btree',
      fields: ['streamer', 'nickname', 'msg', 'created_at']
    })

    // 2. 데이터 삽입
    await db.insert({
      streamer: 'paka',
      nickname: 'user1',
      msg: '안녕하세요 반가워요',
      created_at: 1000
    })

    const fieldsBefore = Array.from((db as any).api.indexedFields)
    console.log('Indexed Fields (Before):', fieldsBefore)
    expect(fieldsBefore).toContain('msg') // msg가 반드시 포함되어야 함

    await db.close()

    // 3. 재시작
    db = DocumentDataply.Define<ChatDoc>().Options({ logLevel: 3 }).Open(DB_PATH)
    await db.init()

    const indexedFields = Array.from((db as any).api.indexedFields)
    console.log('Indexed Fields (After Restart):', indexedFields)

    // 검증: 복합 인덱스의 모든 필드('msg' 포함)가 indexedFields에 정상적으로 존재해야 함
    expect(indexedFields).toContain('streamer')
    expect(indexedFields).toContain('nickname')
    expect(indexedFields).toContain('msg')
    expect(indexedFields).toContain('created_at')

    // undefined나 빈 문자열이 없어야 함
    expect(indexedFields.filter(f => !f || f === 'undefined')).toHaveLength(0)

    // 4. Btree primaryEqual 쿼리 검증 (첫 번째 필드 'streamer' 기준)
    const streamerResults = await db.select({ streamer: 'paka' }).drain()
    expect(streamerResults).toHaveLength(1)
    expect(streamerResults[0].nickname).toBe('user1')

    await db.close()
  })

  test('Should throw error for non-indexed query field', async () => {
    let db = DocumentDataply.Define<ChatDoc>().Options({ logLevel: 3 }).Open(DB_PATH)
    await db.init()

    // 인덱스가 없는 'unknown_field'로 검색 시도
    expect(() => {
      db.select({ unknown_field: 'test' as any })
    }).toThrow(/Query field "unknown_field" is not indexed/)

    await db.close()
  })
})
