import type { DataplyTreeValue, Primitive } from '../../types'
import { BPTreeNode, SerializeStrategyAsync, type SerializeStrategyHead } from 'dataply'
import { DocumentDataplyAPI } from '../document'

export class DocumentSerializeStrategyAsync<T extends Primitive> extends SerializeStrategyAsync<number, DataplyTreeValue<T>> {
  constructor(
    order: number,
    protected readonly api: DocumentDataplyAPI<any>,
    protected readonly txContext: DocumentDataplyAPI<any>['txContext'],
    protected readonly treeKey: string
  ) {
    super(order)
  }

  async id(isLeaf: boolean): Promise<string> {
    const tx = this.txContext.get()!
    const pk = await this.api.insertAsOverflow(new Uint8Array(0), false, tx)
    return pk + ''
  }

  async read(id: string): Promise<BPTreeNode<number, DataplyTreeValue<T>>> {
    const tx = this.txContext.get()!
    const row = await this.api.select(+(id), false, tx)
    if (!row) {
      throw new Error('Node not found')
    }
    return JSON.parse(row)
  }

  async write(id: string, node: BPTreeNode<number, DataplyTreeValue<T>>): Promise<void> {
    const tx = this.txContext.get()!
    const json = JSON.stringify(node)
    await this.api.update(+(id), json, tx)
  }

  async delete(id: string): Promise<void> {
    const tx = this.txContext.get()!
    await this.api.delete(+(id), false, tx)
  }

  async readHead(): Promise<SerializeStrategyHead | null> {
    const tx = this.txContext.get()!
    const metadata = await this.api.getDocumentMetadata(tx)
    if (metadata.treeHeads[this.treeKey]) {
      return metadata.treeHeads[this.treeKey]
    }
    return null
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const tx = this.txContext.get()!
    const metadata = await this.api.getDocumentMetadata(tx)
    metadata.treeHeads[this.treeKey] = head
    await this.api.updateDocumentMetadata(metadata, tx)
  }
}
