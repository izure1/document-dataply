import type { DataplyTreeValue, Primitive } from '../../types'
import {
  type SerializeStrategyHead,
  BPTreeNode,
  SerializeStrategyAsync,
  Ryoiki
} from 'dataply'
import { DocumentDataplyAPI } from '../documentAPI'

export class DocumentSerializeStrategyAsync<T extends Primitive> extends SerializeStrategyAsync<number, DataplyTreeValue<T>> {
  /**
   * readHead에서 할당된 headPk를 캐싱하여
   * writeHead에서 AsyncLocalStorage 컨텍스트 유실 시에도 사용할 수 있도록 함
   */
  private cachedHeadPk: number | null = null

  constructor(
    order: number,
    protected readonly api: DocumentDataplyAPI<any>,
    protected readonly txContext: DocumentDataplyAPI<any>['txContext'],
    public readonly treeKey: string
  ) {
    super(order)
  }

  async id(isLeaf: boolean): Promise<string> {
    const tx = this.txContext.get()
    // 빈 문자열을 사용하는 insertAsOverflow 대신 플레이스홀더 문자열을 사용하는 일반 insert 사용
    const pk = await this.api.insertAsOverflow('__BPTREE_NODE_PLACEHOLDER__', false, tx)
    return pk + ''
  }

  async read(id: string): Promise<BPTreeNode<number, DataplyTreeValue<T>>> {
    const tx = this.txContext.get()
    const row = await this.api.select(Number(id), false, tx)
    if (row === null || row === '' || row.startsWith('__BPTREE_')) {
      throw new Error(`Node not found or empty with ID: ${id}`)
    }
    return JSON.parse(row)
  }

  async write(id: string, node: BPTreeNode<number, DataplyTreeValue<T>>): Promise<void> {
    const tx = this.txContext.get()
    const json = JSON.stringify(node)
    await this.api.update(+(id), json, tx)
  }

  async delete(id: string): Promise<void> {
    const tx = this.txContext.get()
    await this.api.delete(+(id), false, tx)
  }

  async readHead(): Promise<SerializeStrategyHead | null> {
    const tx = this.txContext.get()
    const metadata = await this.api.getDocumentInnerMetadata(tx!)
    const indexInfo = metadata.indices[this.treeKey]

    if (!indexInfo) return null // Document.ts에서 -1로 초기화되었어야 함

    const headPk = indexInfo[0]
    if (headPk === -1) {
      // 지연 생성 메커니즘: 헤드를 위한 행 예약
      const pk = await this.api.insertAsOverflow('__BPTREE_HEAD_PLACEHOLDER__', false, tx!)
      // 실제 PK로 메타데이터 업데이트
      metadata.indices[this.treeKey][0] = pk
      await this.api.updateDocumentInnerMetadata(metadata, tx!)

      // headPk 캐싱 (writeHead에서 사용)
      this.cachedHeadPk = pk

      return null // BPTree 초기화를 트리거하기 위해 null 반환 (루트 생성 및 writeHead 호출)
    }

    // headPk 캐싱 (writeHead에서 사용)
    this.cachedHeadPk = headPk

    const row = await this.api.select(headPk, false, tx)
    // row가 null이거나 빈 문자열이거나 placeholder면 아직 초기화되지 않은 것 → null 반환하여 init 트리거
    if (row === null || row === '' || row.startsWith('__BPTREE_')) return null

    return JSON.parse(row)
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const tx = this.txContext.get()
    let headPk = this.cachedHeadPk

    // 캐시가 없으면 메타데이터에서 읽기 (폴백)
    if (headPk === null) {
      const metadata = await this.api.getDocumentInnerMetadata(tx!)
      const indexInfo = metadata.indices[this.treeKey]
      if (!indexInfo) {
        throw new Error(`Index info not found for tree: ${this.treeKey}. Initialization should be handled outside.`)
      }
      headPk = indexInfo[0]
    }

    const json = JSON.stringify(head)
    await this.api.update(headPk, json, tx)
  }
}
