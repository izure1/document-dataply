import { describe, expect, test, beforeAll, afterAll } from '@jest/globals'
import * as fs from 'node:fs/promises'
import * as path from 'node:path'
import { DocumentDataplyAPI } from '../src/core/documentAPI'

interface FTSDoc {
  [key: string]: any
  content: string
  tags: string[]
}

describe('Full Text Search (FTS)', () => {
  const dbPath = path.join(__dirname, 'test_fts_db.dp')
  let db: DocumentDataplyAPI<FTSDoc, {
    content: {
      type: 'fts',
      tokenizer: 'ngram',
      gramSize: 2
    }
  }>

  beforeAll(async () => {
    // 이전 잔여 파일 제거
    try {
      await fs.unlink(dbPath)
    } catch (e) { }

    db = new DocumentDataplyAPI<FTSDoc, { content: { type: 'fts', tokenizer: 'ngram', gramSize: 2 } }>(dbPath, {
      indices: {
        content: { type: 'fts', tokenizer: 'ngram', gramSize: 2 }
      }
    })
    await db.init()
  })

  afterAll(async () => {
    await db.close()
    try {
      await fs.unlink(dbPath)
    } catch (e) { }
  })

  test('Should index and search using match operator', async () => {
    await db.insertBatchDocuments([
      { content: '안녕하세요 웹 데이터베이스 데이터플라이입니다.', tags: ['intro'] },
      { content: '데이터플라이에서 FTS 검색을 지원합니다.', tags: ['feature'] },
      { content: '검색 엔진처럼 빠르고 가벼운 인덱스', tags: ['feature'] },
      { content: '데이터 처리량 최적화 플라이', tags: ['perf'] }
    ])

    // 기본 단어 검색 전 B+Tree 안의 모든 데이터를 덤프해봅니다
    const contentTree = db.trees.get('content')
    if (contentTree) {
      const allKeys = await contentTree.keys({ primaryGte: { v: '' as any } } as any)
      // 실제 내용물 (k, v)를 모두 뽑아봄
      const stream = contentTree.whereStream({ primaryGte: { v: '' as any } } as any)
      const dump = []
      for await (const [k, value] of stream) {
        dump.push({ k, v: value })
      }
    }

    // 기본 단어 검색
    console.log('Querying for: 데이터플라이')
    let { drain } = db.selectDocuments({ content: { match: '데이터플라이' } })
    let res = await drain()
    expect(res.length).toBe(2)
    expect(res.map(d => d._id).sort()).toEqual([1, 2])
    console.log(res)

    // 교집합 테스트 (다중 토큰 띄어쓰기)
    console.log('Querying for: 데이터 최적화')
    const stream2 = db.selectDocuments({ content: { match: '데이터 최적화' } })
    res = await stream2.drain()
    expect(res.length).toBe(1)
    expect(res[0]._id).toBe(4)
    console.log(res)

    // False Positive (오탐) 테스트 방어 검증
    // "데"와 "이"는 "데이터베이스"나 "데이터플라이"에서 각각 추출되지만 원문 "데 이"로는 없으므로 안나와야 함
    // (현재 tokenizer는 2-gram이므로 '데이' 만 존재함)
    console.log('Querying for: 없는단어')
    const stream3 = db.selectDocuments({ content: { match: '없는단어' } })
    res = await stream3.drain()
    expect(res.length).toBe(0)
  })

  test('Should handle Update and Delete correctly in FTS', async () => {
    // 문서 수정
    await db.partialUpdate({ _id: 1 }, { content: '수정된 첫 인사입니다.' })
    let { drain } = db.selectDocuments({ content: { match: '데이터플라이' } })
    let res = await drain()
    expect(res.length).toBe(1)
    expect(res[0]._id).toBe(2) // 1번 문서는 수정되었으므로 검색 안됨

    // 기존 문서가 새 키워드로 검색되는지
    const stream2 = db.selectDocuments({ content: { match: '수정된' } })
    res = await stream2.drain()
    expect(res.length).toBe(1)
    expect(res[0]._id).toBe(1)

    // 문서 삭제
    await db.deleteDocuments({ _id: 2 })
    const stream3 = db.selectDocuments({ content: { match: '검색을 지원합니다' } })
    res = await stream3.drain()
    expect(res.length).toBe(0) // 2번 삭제됨
  })
})
