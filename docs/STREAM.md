# Stream Guide: High-Performance Data Processing (STREAM.md)

`document-dataply` is designed to operate safely without memory overhead even when processing millions of documents. This document explains the core design philosophy of streaming data processing and its technical background.

---

## 1. Core Mechanism: Stream (Async Iterator)

The most recommended way to retrieve data is through **`stream`**. This method does not load all data into memory at once; instead, it reads documents asynchronously one by one as needed.

### Key Advantages
- **Extremely Low Memory Usage**: Even with tens of thousands of results, only the document currently being processed is held in memory.
- **Immediate Responsiveness**: Processing can begin as soon as the first document is loaded, without waiting for the entire dataset to be read.

> [!CAUTION]
> **`orderBy` Negates Stream Benefits**: When `orderBy` is specified, the streaming performance advantage is lost. This is because the underlying B+Tree structure returns documents in an internal order, not the requested sort order. To fulfill the `orderBy` request, all matching documents must first be loaded into memory, sorted, and then returned sequentially. Therefore, for optimal streaming performance, avoid using `orderBy` when processing large datasets.

### Usage Example
You can easily use the `for await...of` syntax to iterate through results as if they were in a standard array.

```typescript
const query = db.select({ status: 'active' });

for await (const doc of query.stream) {
  // Only one document is loaded in memory at a time
  console.log(`ID: ${doc._id}, Name: ${doc.name}`);
  await processData(doc);
}
```

---

## 2. Alternative: Drain (Array)

If you prefer to receive all results at once in a standard array format, use the **`drain()`** method.

### Characteristics and Limitations
- **Convenience**: Since the return value is an array, you can immediately use standard JavaScript array methods like `.map()`, `.filter()`, and `.reduce()`.
- **Precaution**: If the result set is very large, the application may terminate due to an Out of Memory (OOM) error. Always prefer `stream` when the amount of data is uncertain.

### Internal Implementation
`drain()` is not a completely separate logic. Internally, it is implemented to iterate through the `stream` described above until the end, collecting all results into an array. Therefore, it benefits equally from the page loading and cache optimizations described below.

```typescript
// Useful for processing small amounts of data
const activeUsers = await db.select({ role: 'admin' }).drain();
const names = activeUsers.map(u => u.name);
```

---

## 3. Technical Details: Internal Principles

`document-dataply` follows the design principles of modern Relational Database Management Systems (RDBMS) for high Input/Output (IO) performance.

### Page-Unit Loading
When retrieving documents from storage, the engine does not read from the disk entry by entry. Instead, it reads data in **Page units** for efficiency. This minimizes unnecessary disk I/O and significantly increases processing speed.

### LRU (Least Recently Used) Cache Strategy
Loaded pages are managed within the internal buffer pool of the engine.
- **Efficient Resource Management**: The LRU algorithm ensures that frequently accessed data remains in memory, while least recently used data is evicted, guaranteeing efficient resource utilization.
- **Consistent Performance**: Frequently repeated queries leverage cached pages for excellent response times.

---

## 4. Practical Usage Patterns

### Bulk Updates and Deletions
Modification operations such as `partialUpdate`, `fullUpdate`, and `delete` also utilize this streaming mechanism internally. This is why heavy tasks like deleting millions of records can be performed safely without memory concerns.

```typescript
// Safe for large datasets as it operates via streaming internally
const deletedCount = await db.delete({ status: 'old' });
```

### Batch Processing (Chunking) Pattern
If you want to use a stream but process data in batches (e.g., sending groups of documents to an external API), you can implement a chunking pattern:

```typescript
let chunk = [];
const CHUNK_SIZE = 500;

for await (const doc of db.select({ type: 'export' }).stream) {
  chunk.push(doc);
  if (chunk.length >= CHUNK_SIZE) {
    await sendBatchToCloud(chunk);
    chunk = [];
  }
}

// Process remaining data
if (chunk.length > 0) await sendBatchToCloud(chunk);
```

---

## 5. Conclusion: Choosing the Right Method

- **Always consider `stream` first**: It is optimal for server-side environments where the data volume is unpredictable or where memory efficiency is critical.
- **Use `drain()` only for small datasets**: Choose this method when the result set is guaranteed to be small (e.g., a few hundred rows) and quick implementation is preferred.
