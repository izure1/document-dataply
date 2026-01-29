import type { DataplyTreeValue, Primitive } from '../../types'
import { BPTreeNode, SerializeStrategyAsync, type SerializeStrategyHead } from 'dataply'
import { DocumentDataplyAPI } from '../document'

export class DocumentSerializeStrategyAsync<T extends Primitive> extends SerializeStrategyAsync<number, DataplyTreeValue<T>> {
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
    // BPTree가 id() 후에 바로 노드를 write하기 전에 read할 수 있으므로
    // 빈 문자열 대신 placeholder 노드 구조를 저장
    const placeholder = JSON.stringify({
      id: '',  // 임시, write 시 덮어씀
      keys: [],
      values: [],
      leaf: isLeaf,
      parent: null,
      next: null,
      prev: null
    })
    const pk = await this.api.insertAsOverflow(placeholder, false, tx)
    return pk + ''
  }


  async read(id: string): Promise<BPTreeNode<number, DataplyTreeValue<T>>> {
    const tx = this.txContext.get()
    const row = await this.api.select(Number(id), false, tx)
    if (row === null || row === '') {
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
    const indexInfo = metadata.indecies[this.treeKey]

    if (!indexInfo) return null // Should have been initialized with 0 in Document.ts

    let headPk = indexInfo[0]

    if (headPk === 0) {
      // Lazy creation mechanism: Reserve a row for the head
      const pk = await this.api.insertAsOverflow('', false, tx!)
      // Update metadata with the real PK
      metadata.indecies[this.treeKey][0] = pk
      await this.api.updateDocumentInnerMetadata(metadata, tx!)

      return null // Return null to trigger BPTree initialization (creates root, calls writeHead)
    }

    const row = await this.api.select(headPk, false, tx)
    if (row === null) return null

    return JSON.parse(row)
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const tx = this.txContext.get()
    const metadata = await this.api.getDocumentInnerMetadata(tx!)
    const indexInfo = metadata.indecies[this.treeKey]

    if (!indexInfo) {
      throw new Error(`Index info not found for tree: ${this.treeKey}. Initialization should be handled outside.`)
    }
    const headPk = indexInfo[0]
    const json = JSON.stringify(head)

    await this.api.update(headPk, json, tx)
  }

  /**
   * Compare-and-Swap for head.
   * Document DB에서는 각 tree의 head가 별도 row에 저장되고,
   * 상위 Document 트랜잭션으로 일관성이 관리됩니다.
   * 따라서 BPTree 트랜잭션들은 서로 충돌하지 않으므로 항상 성공합니다.
   */
  async compareAndSwapHead(oldRoot: string | null, newRoot: string): Promise<boolean> {
    console.log('[DEBUG] compareAndSwapHead called:', { treeKey: this.treeKey, oldRoot, newRoot })
    this.head.root = newRoot
    await this.writeHead(this.head)
    console.log('[DEBUG] compareAndSwapHead success')
    return true
  }
}
